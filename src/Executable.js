// SPDX-License-Identifier: Apache-2.0

import List from "./transaction/List.js";
import RetryExecutor, { ExecutionState, RST_STREAM } from "./RetryExecutor.js";

// Re-export for backward compatibility — consumers import these from Executable.js
export { ExecutionState, RST_STREAM };

/**
 * @typedef {import("./account/AccountId.js").default} AccountId
 * @typedef {import("./channel/Channel.js").default} Channel
 * @typedef {import("./channel/MirrorChannel.js").default} MirrorChannel
 * @typedef {import("./transaction/TransactionId.js").default} TransactionId
 * @typedef {import("./client/Client.js").ClientOperator} ClientOperator
 * @typedef {import("./Node.js").default} Node
 * @typedef {import("./Signer.js").Signer} Signer
 * @typedef {import("./PublicKey.js").default} PublicKey
 * @typedef {import("./Status.js").default} Status
 * @typedef {import("./logger/Logger.js").default} Logger
 */

/**
 * @abstract
 * @internal
 * @template RequestT
 * @template ResponseT
 * @template OutputT
 */
export default class Executable {
    constructor() {
        /**
         * The number of times we can retry the grpc call
         *
         * @internal
         * @type {?number}
         */
        this._maxAttempts = null;

        /**
         * List of node account IDs for each transaction that has been
         * built.
         *
         * @internal
         * @type {List<AccountId>}
         */
        this._nodeAccountIds = new List();

        /**
         * List of the transaction node account IDs to check if
         * the node account ID of the request is in the list
         *
         * @protected
         * @type {Array<string>}
         */
        this.transactionNodeIds = [];

        /**
         * @internal
         */
        this._signOnDemand = false;

        /**
         * This is the request's min backoff
         *
         * @internal
         * @type {number | null}
         */
        this._minBackoff = null;

        /**
         * This is the request's max backoff
         *
         * @internal
         * @type {number}
         */
        this._maxBackoff = 8000;

        /**
         * The operator that was used to execute this request.
         * The reason we save the operator in the request is because of the signing on
         * demand feature. This feature requires us to sign new request on each attempt
         * meaning if a client with an operator was used we'd need to sign with the operator
         * on each attempt.
         *
         * @internal
         * @type {ClientOperator | null}
         */
        this._operator = null;

        /**
         * The complete timeout for running the `execute()` method
         *
         * @internal
         * @type {number | null}
         */
        this._requestTimeout = null;

        /**
         * The grpc request timeout aka deadline.
         *
         * The reason we have this is because there were times that consensus nodes held the grpc
         * connection, but didn't return anything; not error nor regular response. This resulted
         * in some weird behavior in the SDKs. To fix this we've added a grpc deadline to prevent
         * nodes from stalling the executing of a request.
         *
         * @internal
         * @type {number | null}
         */
        this._grpcDeadline = null;

        /**
         * Logger
         *
         * @protected
         * @type {Logger | null}
         */
        this._logger = null;
    }

    /**
     * Get the list of node account IDs on the request. If no nodes are set, then null is returned.
     * The reasoning for this is simply "legacy behavior".
     *
     * @returns {?AccountId[]}
     */
    get nodeAccountIds() {
        if (this._nodeAccountIds.isEmpty) {
            return null;
        } else {
            this._nodeAccountIds.setLocked();
            return this._nodeAccountIds.list;
        }
    }

    /**
     * Set the node account IDs on the request
     *
     * @param {AccountId[]} nodeIds
     * @returns {this}
     */
    setNodeAccountIds(nodeIds) {
        // Set the node account IDs, and lock the list. This will require `execute`
        // to use these nodes instead of random nodes from the network.
        this._nodeAccountIds.setList(nodeIds).setLocked();
        return this;
    }

    /**
     * @deprecated
     * @returns {?number}
     */
    get maxRetries() {
        console.warn("Deprecated: use maxAttempts instead");
        return this.maxAttempts;
    }

    /**
     * @param {?number} maxRetries
     * @returns {this}
     */
    setMaxRetries(maxRetries) {
        console.warn("Deprecated: use setMaxAttempts() instead");
        return this.setMaxAttempts(maxRetries);
    }

    /**
     * Get the max attempts on the request
     *
     * @returns {?number}
     */
    get maxAttempts() {
        return this._maxAttempts;
    }

    /**
     * Set the max attempts on the request
     *
     * @param {?number} maxAttempts
     * @returns {this}
     */
    setMaxAttempts(maxAttempts) {
        this._maxAttempts = maxAttempts;

        return this;
    }

    /**
     * Get the grpc deadline
     *
     * @returns {?number}
     */
    get grpcDeadline() {
        return this._grpcDeadline;
    }

    /**
     * Set the grpc deadline
     *
     * @param {number} grpcDeadline
     * @returns {this}
     */
    setGrpcDeadline(grpcDeadline) {
        this._grpcDeadline = grpcDeadline;

        return this;
    }

    /**
     * Set the min backoff for the request
     *
     * @param {number} minBackoff
     * @returns {this}
     */
    setMinBackoff(minBackoff) {
        // Honestly we shouldn't be checking for null since that should be TypeScript's job.
        // Also verify that min backoff is not greater than max backoff.
        if (minBackoff == null) {
            throw new Error("minBackoff cannot be null.");
        } else if (this._maxBackoff != null && minBackoff > this._maxBackoff) {
            throw new Error("minBackoff cannot be larger than maxBackoff.");
        }
        this._minBackoff = minBackoff;
        return this;
    }

    /**
     * Get the min backoff
     *
     * @returns {number | null}
     */
    get minBackoff() {
        return this._minBackoff;
    }

    /**
     * Set the max backoff for the request
     *
     * @param {?number} maxBackoff
     * @returns {this}
     */
    setMaxBackoff(maxBackoff) {
        // Honestly we shouldn't be checking for null since that should be TypeScript's job.
        // Also verify that max backoff is not less than min backoff.
        if (maxBackoff == null) {
            throw new Error("maxBackoff cannot be null.");
        } else if (this._minBackoff != null && maxBackoff < this._minBackoff) {
            throw new Error("maxBackoff cannot be smaller than minBackoff.");
        }
        this._maxBackoff = maxBackoff;
        return this;
    }

    /**
     * Get the max backoff
     *
     * @returns {number}
     */
    get maxBackoff() {
        return this._maxBackoff;
    }

    /**
     * This method is responsible for doing any work before the executing process begins.
     * For paid queries this will result in executing a cost query, for transactions this
     * will make sure we save the operator and sign any requests that need to be signed
     * in case signing on demand is disabled.
     *
     * @abstract
     * @protected
     * @param {import("./client/Client.js").default<Channel, MirrorChannel>} client
     * @returns {Promise<void>}
     */
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _beforeExecute(client) {
        throw new Error("not implemented");
    }

    /**
     * Create a protobuf request which will be passed into the `_execute()` method
     *
     * @abstract
     * @protected
     * @returns {Promise<RequestT>}
     */
    _makeRequestAsync() {
        throw new Error("not implemented");
    }

    /**
     * This name is a bit wrong now, but the purpose of this method is to map the
     * request and response into an error. This method will only be called when
     * `_getStatusAndExecutionState()` returned execution state `ExecutionState.Error`
     *
     * @abstract
     * @internal
     * @param {RequestT} request
     * @param {ResponseT} response
     * @param {AccountId} nodeId
     * @returns {Error}
     */
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _mapStatusError(request, response, nodeId) {
        throw new Error("not implemented");
    }

    /**
     * Map the request, response, and the node account ID used for this attempt into a response.
     * This method will only be called when `_shouldRetry` returned `ExecutionState.Finished`
     *
     * @abstract
     * @protected
     * @param {ResponseT} response
     * @param {AccountId} nodeAccountId
     * @param {RequestT} request
     * @returns {Promise<OutputT>}
     */
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _mapResponse(response, nodeAccountId, request) {
        throw new Error("not implemented");
    }

    /**
     * Perform a single grpc call with the given request. Each request has it's own
     * required service so we just pass in channel, and it'$ the request's responsiblity
     * to use the right service and call the right grpc method.
     *
     * @abstract
     * @internal
     * @param {Channel} channel
     * @param {RequestT} request
     * @returns {Promise<ResponseT>}
     */
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _execute(channel, request) {
        throw new Error("not implemented");
    }

    /**
     * Return the current transaction ID for the request. All requests which are
     * use the same transaction ID for each node, but the catch is that `Transaction`
     * implicitly supports chunked transactions. Meaning there could be multiple
     * transaction IDs stored in the request, and a different transaction ID will be used
     * on subsequent calls to `execute()`
     *
     * FIXME: This method can most likely be removed, although some further inspection
     * is required.
     *
     * @abstract
     * @protected
     * @returns {TransactionId}
     */
    _getTransactionId() {
        throw new Error("not implemented");
    }

    /**
     * Return the log ID for this particular request
     *
     * Log IDs are simply a string constructed to make it easy to track each request's
     * execution even when mulitple requests are executing in parallel. Typically, this
     * method returns the format of `[<request type>.<timestamp of the transaction ID>]`
     *
     * Maybe we should deduplicate this using ${this.consturtor.name}
     *
     * @abstract
     * @internal
     * @returns {string}
     */
    _getLogId() {
        throw new Error("not implemented");
    }

    /**
     * Serialize the request into bytes
     *
     * @abstract
     * @param {RequestT} request
     * @returns {Uint8Array}
     */
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _requestToBytes(request) {
        throw new Error("not implemented");
    }

    /**
     * Serialize the response into bytes
     *
     * @abstract
     * @param {ResponseT} response
     * @returns {Uint8Array}
     */
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _responseToBytes(response) {
        throw new Error("not implemented");
    }

    /**
     * Determine if we should continue the execution process, error, or finish.
     *
     * FIXME: This method should really be called something else. Initially it returned
     * a boolean so `shouldRetry` made sense, but now it returns an enum, so the name
     * no longer makes sense.
     *
     * @abstract
     * @protected
     * @param {RequestT} request
     * @param {ResponseT} response
     * @returns {[Status, ExecutionState]}
     */
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _getStatusAndExecutionState(request, response) {
        throw new Error("not implemented");
    }

    /**
     * A helper method for setting the operator on the request
     *
     * @internal
     * @param {AccountId} accountId
     * @param {PublicKey} publicKey
     * @param {(message: Uint8Array) => Promise<Uint8Array>} transactionSigner
     * @returns {this}
     */
    _setOperatorWith(accountId, publicKey, transactionSigner) {
        this._operator = {
            transactionSigner,
            accountId,
            publicKey,
        };
        return this;
    }

    /**
     * Execute this request using the signer
     *
     * This method is part of the signature providers feature
     * https://hips.hedera.com/hip/hip-338
     *
     * @param {Signer} signer
     * @returns {Promise<OutputT>}
     */
    async executeWithSigner(signer) {
        return signer.call(this);
    }

    /**
     * @returns {boolean}
     * @abstract
     * @protected
     */
    isBatchedAndNotBatchTransaction() {
        return false;
    }

    /**
     * @private
     * @returns {void}
     */
    _validateTransactionNodeIds() {
        if (!this.transactionNodeIds.length) {
            return;
        }

        const nodeAccountIds = this._nodeAccountIds.list.map((nodeId) =>
            nodeId.toString(),
        );

        const hasValidNodes = this.transactionNodeIds.some((nodeId) =>
            nodeAccountIds.includes(nodeId),
        );

        if (hasValidNodes) {
            return;
        }

        const displayNodeAccountIds =
            nodeAccountIds.length > 2
                ? `${nodeAccountIds.slice(0, 2).join(", ")} ...`
                : nodeAccountIds.join(", ");
        const isSingleNode = nodeAccountIds.length === 1;

        throw new Error(
            `Attempting to execute a transaction against node${
                isSingleNode ? "" : "s"
            } ${displayNodeAccountIds}, ` +
                `which ${
                    isSingleNode ? "is" : "are"
                } not included in the Client's node list. Please review your Client configuration.`,
        );
    }

    /**
     * @private
     * @template {Channel} ChannelT
     * @template {MirrorChannel} MirrorChannelT
     * @param {import("./client/Client.js").default<ChannelT, MirrorChannelT>} client
     * @param {number=} requestTimeout
     * @returns {Promise<void>}
     */
    async _setupExecution(client, requestTimeout) {
        if (this.isBatchedAndNotBatchTransaction()) {
            throw new Error(
                "Cannot execute batchified transaction outside of BatchTransaction",
            );
        }

        // If the logger on the request is not set, use the logger in client
        // (if set, otherwise do not use logger)
        this._logger = this._logger ?? client._logger;

        // If the request timeout is set on the request we'll prioritize that instead
        // of the parameter provided, and if the parameter isn't provided we'll
        // use the default request timeout on client
        if (this._requestTimeout == null) {
            this._requestTimeout = requestTimeout || client.requestTimeout;
        }

        // If the grpc deadline is not set on the request, use the default value from client
        if (this._grpcDeadline == null) {
            this._grpcDeadline = client.grpcDeadline;
        }

        // If the max backoff on the request is not set, use the default value in client
        if (this._maxBackoff == null) {
            this._maxBackoff = client.maxBackoff;
        }

        // If the min backoff on the request is not set, use the default value in client
        if (this._minBackoff == null) {
            this._minBackoff = client.minBackoff;
        }

        if (this._maxAttempts == null) {
            this._maxAttempts = client.maxAttempts;
        }

        // Some request need to perform additional requests before the executing
        // such as paid queries need to fetch the cost of the query before
        // finally executing the actual query.
        await this._beforeExecute(client);
    }

    /**
     * This is used to skip the current node if the node account ID is not valid for the transaction.
     * @private
     * @param {AccountId} nodeAccountId
     * @returns {boolean}
     */
    _shouldSkipAttemptForNodeAccountId(nodeAccountId) {
        if (!this.transactionNodeIds.length) {
            return false;
        }

        const isNodeAccountIdValid = this.transactionNodeIds.includes(
            nodeAccountId.toString(),
        );

        if (isNodeAccountIdValid) {
            return false;
        }

        return true;
    }

    /**
     * Execute the request using a client and an optional request timeout
     *
     * @template {Channel} ChannelT
     * @template {MirrorChannel} MirrorChannelT
     * @param {import("./client/Client.js").default<ChannelT, MirrorChannelT>} client
     * @param {number=} requestTimeout
     * @returns {Promise<OutputT>}
     */
    async execute(client, requestTimeout) {
        await this._setupExecution(client, requestTimeout);

        // Checks if has a valid nodes to which the TX can be sent
        this._validateTransactionNodeIds();

        const retryExecutor = new RetryExecutor({
            maxAttempts: /** @type {number} */ (this._maxAttempts),
            requestTimeout: this._requestTimeout,
            grpcDeadline: this._grpcDeadline,
            minBackoff: /** @type {number} */ (this._minBackoff),
            maxBackoff: this._maxBackoff,
            logger: this._logger,
        });

        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        return retryExecutor.execute(this, client);
    }

    /**
     * The current purpose of this method is to easily support signature providers since
     * signature providers need to serialize _any_ request into bytes. `Query` and `Transaction`
     * already implement `toBytes()` so it only made sense to make it available here too.
     *
     * @abstract
     * @returns {Uint8Array}
     */
    toBytes() {
        throw new Error("not implemented");
    }

    /**
     * Set logger
     *
     * @param {Logger} logger
     * @returns {this}
     */
    setLogger(logger) {
        this._logger = logger;
        return this;
    }

    /**
     * Get logger if set
     *
     * @returns {?Logger}
     */
    get logger() {
        return this._logger;
    }
}
