// SPDX-License-Identifier: Apache-2.0

import GrpcServiceError from "./grpc/GrpcServiceError.js";
import GrpcStatus from "./grpc/GrpcStatus.js";
import HttpError from "./http/HttpError.js";
import Status from "./Status.js";
import MaxAttemptsOrTimeoutError from "./MaxAttemptsOrTimeoutError.js";
import * as hex from "./encoding/hex.js";

/**
 * @typedef {import("./account/AccountId.js").default} AccountId
 * @typedef {import("./channel/Channel.js").default} Channel
 * @typedef {import("./channel/MirrorChannel.js").default} MirrorChannel
 * @typedef {import("./transaction/List.js").default<AccountId>} NodeAccountIdList
 * @typedef {import("./Node.js").default} Node
 * @typedef {import("./logger/Logger.js").default} Logger
 */

/**
 * @enum {string}
 */
export const ExecutionState = {
    Finished: "Finished",
    Retry: "Retry",
    Error: "Error",
};

export const RST_STREAM = /\brst[^0-9a-zA-Z]stream\b/i;

/**
 * Encapsulates the retry loop logic extracted from Executable.execute().
 *
 * The strategy parameter is the Executable instance itself — it already has
 * all the abstract methods that subclasses (Transaction, Query) override.
 *
 * @template RequestT
 * @template ResponseT
 * @template OutputT
 */
export default class RetryExecutor {
    /**
     * @param {object} config
     * @param {number} config.maxAttempts
     * @param {number | null} config.requestTimeout
     * @param {number | null} config.grpcDeadline
     * @param {number} config.minBackoff
     * @param {number} config.maxBackoff
     * @param {Logger | null} config.logger
     */
    constructor(config) {
        /** @type {number} */
        this._maxAttempts = config.maxAttempts;
        /** @type {number | null} */
        this._requestTimeout = config.requestTimeout;
        /** @type {number | null} */
        this._grpcDeadline = config.grpcDeadline;
        /** @type {number} */
        this._minBackoff = config.minBackoff;
        /** @type {number} */
        this._maxBackoff = config.maxBackoff;
        /** @type {Logger | null} */
        this._logger = config.logger;
    }

    /**
     * Execute with retry logic, delegating request/response handling to the strategy.
     *
     * @param {import("./Executable.js").default<RequestT, ResponseT, OutputT>} strategy
     * @template {Channel} ChannelT
     * @template {MirrorChannel} MirrorChannelT
     * @param {import("./client/Client.js").default<ChannelT, MirrorChannelT>} client
     * @returns {Promise<OutputT>}
     */
    async execute(strategy, client) {
        const nodeAccountIds = strategy._nodeAccountIds;
        const requestStartTime = Date.now();
        /** @type {Error | null} */
        let persistentError = null;

        for (let attempt = 1; attempt <= this._maxAttempts; attempt++) {
            // Phase 1: Timeout check
            if (
                this._requestTimeout != null &&
                requestStartTime + this._requestTimeout <= Date.now()
            ) {
                throw new MaxAttemptsOrTimeoutError(
                    "timeout exceeded",
                    nodeAccountIds.isEmpty
                        ? "No node account ID set"
                        : nodeAccountIds.current.toString(),
                );
            }

            // Phase 2: Skip invalid nodes
            if (
                // @ts-ignore -- accessing private method via strategy pattern
                strategy._shouldSkipAttemptForNodeAccountId(
                    nodeAccountIds.current,
                )
            ) {
                console.error(
                    `Attempting to execute a transaction against node ${nodeAccountIds.current.toString()}, which is not included in the Client's node list. Please review your Client configuration.`,
                );

                nodeAccountIds.advance();
                continue;
            }

            // Phase 3: Node selection
            const executionNode = this._getExecutionNode(
                nodeAccountIds,
                client,
            );

            this._logger?.debug(
                `[${strategy._getLogId()}] Node AccountID: ${executionNode.accountId.toString()}, IP: ${executionNode.address.toString()}`,
            );

            const channel = executionNode.getChannel();

            if (this._grpcDeadline != null) {
                channel.setGrpcDeadline(this._grpcDeadline);
            }

            // Phase 4: Build request
            // @ts-ignore -- accessing protected method via strategy pattern
            const request = await strategy._makeRequestAsync();

            // Phase 5: Health check
            if (!executionNode.isHealthy()) {
                await this._handleUnhealthyNode(
                    strategy,
                    executionNode,
                    request,
                    attempt,
                    nodeAccountIds,
                    client,
                );
                continue;
            }

            // Phase 6: Advance node for next iteration
            nodeAccountIds.advance();

            // Phase 7: Execute request
            let response;
            try {
                response = await this._executeRequestWithGrpcDeadline(
                    strategy,
                    channel,
                    request,
                );
            } catch (err) {
                const error = GrpcServiceError._fromResponse(
                    /** @type {Error} */ (err),
                );

                persistentError = error;

                this._logger?.debug(
                    `[${strategy._getLogId()}] received error ${JSON.stringify(
                        error,
                    )}`,
                );

                if (this._shouldRetryRequestError(strategy, error, attempt)) {
                    this._logger?.debug(
                        `[${strategy._getLogId()}] node with accountId: ${executionNode.accountId.toString()} and proxy IP: ${executionNode.address.toString()} is unhealthy`,
                    );

                    if (executionNode.isHealthy()) {
                        client._network.increaseBackoff(executionNode);
                    }
                    continue;
                }

                throw err;
            }

            this._logger?.trace(
                `[${strategy._getLogId()}] sending protobuf ${hex.encode(
                    strategy._responseToBytes(response),
                )}`,
            );

            // Phase 8: Decrease backoff on success
            client._network.decreaseBackoff(executionNode);

            // Phase 9: Evaluate response
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
            const [status, shouldRetry] = /** @type {[Status, string]} */ (
                // @ts-ignore -- accessing protected method via strategy pattern
                strategy._getStatusAndExecutionState(request, response)
            );

            const isError =
                status.toString() !== Status.Ok.toString() &&
                status.toString() !== Status.Success.toString();

            if (isError) {
                persistentError = new Error(status.toString());
            }

            // Phase 10: Branch on execution state
            switch (shouldRetry) {
                case ExecutionState.Retry:
                    if (status === Status.InvalidNodeAccount) {
                        await this._handleInvalidNodeAccountId(
                            strategy,
                            client,
                            executionNode,
                            executionNode.accountId,
                        );
                    }

                    await delayForAttempt(
                        client.isLocalNetwork,
                        attempt,
                        this._minBackoff,
                        this._maxBackoff,
                    );
                    continue;
                case ExecutionState.Finished:
                    // @ts-ignore -- accessing protected method via strategy pattern
                    return strategy._mapResponse(
                        response,
                        executionNode.accountId,
                        request,
                    );
                case ExecutionState.Error:
                    throw strategy._mapStatusError(
                        request,
                        response,
                        executionNode.accountId,
                    );
                default:
                    throw new Error(
                        "(BUG) non-exhaustive switch statement for `ExecutionState`",
                    );
            }
        }

        throw new MaxAttemptsOrTimeoutError(
            `max attempts of ${this._maxAttempts.toString()} was reached for request with last error being: ${
                persistentError != null ? persistentError.toString() : ""
            }`,
            nodeAccountIds.current.toString(),
        );
    }

    /**
     * @private
     * @param {NodeAccountIdList} nodeAccountIds
     * @param {import("./client/Client.js").default<Channel, MirrorChannel>} client
     * @returns {Node}
     */
    _getExecutionNode(nodeAccountIds, client) {
        /** @type {Node} */
        let currentNode;

        if (nodeAccountIds.isEmpty) {
            currentNode = /** @type {Node} */ (client._network.getNode());
            nodeAccountIds.setList([currentNode.accountId]);
        } else {
            currentNode = /** @type {Node} */ (
                client._network.getNode(nodeAccountIds.current)
            );
        }

        if (currentNode == null) {
            throw new Error(
                `NodeAccountId not recognized: ${nodeAccountIds.current.toString()}`,
            );
        }

        return currentNode;
    }

    /**
     * @private
     * @param {import("./Executable.js").default<RequestT, ResponseT, OutputT>} strategy
     * @param {Node} currentNode
     * @param {RequestT} request
     * @param {number} attempt
     * @param {NodeAccountIdList} nodeAccountIds
     * @param {import("./client/Client.js").default<Channel, MirrorChannel>} client
     * @returns {Promise<void>}
     */
    async _handleUnhealthyNode(
        strategy,
        currentNode,
        request,
        attempt,
        nodeAccountIds,
        client,
    ) {
        const isSingleNodeReceiptOrRecordRequest =
            isTransactionReceiptOrRecordRequest(request) &&
            nodeAccountIds.length <= 1;

        if (isSingleNodeReceiptOrRecordRequest || client.isLocalNetwork) {
            await delayForAttempt(
                client.isLocalNetwork,
                attempt,
                this._minBackoff,
                this._maxBackoff,
            );
            return;
        }

        const isLastNode =
            nodeAccountIds.index === nodeAccountIds.list.length - 1;

        if (isLastNode) {
            throw new Error(
                `Network connectivity issue: All nodes are unhealthy. Original node list: ${nodeAccountIds.list.join(
                    ", ",
                )}`,
            );
        }

        this._logger?.debug(
            `[${strategy._getLogId()}] Node is not healthy, trying the next node.`,
        );
        nodeAccountIds.advance();
    }

    /**
     * @private
     * @param {import("./Executable.js").default<RequestT, ResponseT, OutputT>} strategy
     * @param {import("./client/Client.js").default<Channel, MirrorChannel>} client
     * @param {Node} currentNode
     * @param {AccountId} nodeAccountId
     * @returns {Promise<void>}
     */
    async _handleInvalidNodeAccountId(
        strategy,
        client,
        currentNode,
        nodeAccountId,
    ) {
        this._logger?.debug(
            `[${strategy._getLogId()}] node with accountId: ${nodeAccountId.toString()} and proxy IP: ${currentNode.address.toString()} has invalid node account ID, marking as unhealthy and updating network`,
        );

        client._network.increaseBackoff(currentNode);

        try {
            if (client.mirrorNetwork.length > 0) {
                await client.updateNetwork();
            } else {
                this._logger?.warn(
                    `[${strategy._getLogId()}] Cannot update address book: no mirror network configured. Retrying with existing network configuration.`,
                );
            }
        } catch (error) {
            const errorMessage =
                error instanceof Error ? error.message : String(error);
            this._logger?.trace(
                `[${strategy._getLogId()}] failed to update client address book after INVALID_NODE_ACCOUNT_ID: ${errorMessage}`,
            );
        }
    }

    /**
     * @private
     * @param {import("./Executable.js").default<RequestT, ResponseT, OutputT>} strategy
     * @param {Channel} channel
     * @param {RequestT} request
     * @returns {Promise<ResponseT>}
     */
    async _executeRequestWithGrpcDeadline(strategy, channel, request) {
        const promises = [];
        /** @type {ReturnType<typeof setTimeout> | null} */
        let deadlineTimer = null;

        if (this._grpcDeadline != null) {
            promises.push(
                new Promise((_, reject) => {
                    deadlineTimer = setTimeout(
                        () =>
                            reject(
                                new GrpcServiceError(
                                    GrpcStatus.DeadlineExceeded,
                                ),
                            ),
                        /** @type {number=} */ (this._grpcDeadline),
                    );
                }),
            );
        }

        this._logger?.trace(
            `[${strategy._getLogId()}] sending protobuf ${hex.encode(
                strategy._requestToBytes(request),
            )}`,
        );

        promises.push(strategy._execute(channel, request));

        try {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-return
            return /** @type {ResponseT} */ (await Promise.race(promises));
        } finally {
            if (deadlineTimer != null) {
                clearTimeout(deadlineTimer);
            }
        }
    }

    /**
     * @private
     * @param {import("./Executable.js").default<RequestT, ResponseT, OutputT>} strategy
     * @param {Error} error
     * @param {number} attempt
     * @returns {boolean}
     */
    _shouldRetryRequestError(strategy, error, attempt) {
        return (
            (error instanceof GrpcServiceError || error instanceof HttpError) &&
            this._shouldRetryExceptionally(error) &&
            attempt <= this._maxAttempts
        );
    }

    /**
     * @private
     * @param {Error} error
     * @returns {boolean}
     */
    _shouldRetryExceptionally(error) {
        if (error instanceof GrpcServiceError) {
            return (
                error.status._code === GrpcStatus.Timeout._code ||
                error.status._code === GrpcStatus.DeadlineExceeded._code ||
                error.status._code === GrpcStatus.Unavailable._code ||
                error.status._code === GrpcStatus.ResourceExhausted._code ||
                error.status._code === GrpcStatus.GrpcWeb._code ||
                (error.status._code === GrpcStatus.Internal._code &&
                    RST_STREAM.test(error.message))
            );
        } else {
            return true;
        }
    }
}

/**
 * Checks if the request is a transaction receipt or record request
 *
 * @template T
 * @param {T} request
 * @returns {boolean}
 */
function isTransactionReceiptOrRecordRequest(request) {
    if (typeof request !== "object" || request === null) {
        return false;
    }

    return (
        "transactionGetReceipt" in request || "transactionGetRecord" in request
    );
}

/**
 * A simple function that returns a promise timeout for a specific period of time
 *
 * @param {boolean} isLocalNode
 * @param {number} attempt
 * @param {number} minBackoff
 * @param {number} maxBackoff
 * @returns {Promise<void>}
 */
function delayForAttempt(isLocalNode, attempt, minBackoff, maxBackoff) {
    if (isLocalNode) {
        return new Promise((resolve) => setTimeout(resolve, minBackoff));
    }

    const ms = Math.min(
        Math.floor(minBackoff * Math.pow(2, attempt)),
        maxBackoff,
    );
    return new Promise((resolve) => setTimeout(resolve, ms));
}
