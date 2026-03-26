/* global globalThis */

import { vi } from "vitest";
import RetryExecutor from "../../src/RetryExecutor.js";
import AccountId from "../../src/account/AccountId.js";
import GrpcServiceError from "../../src/grpc/GrpcServiceError.js";
import GrpcStatus from "../../src/grpc/GrpcStatus.js";
import List from "../../src/transaction/List.js";

/**
 * @returns {RetryExecutor}
 */
function createRetryExecutor(overrides = {}) {
    return new RetryExecutor({
        maxAttempts: 3,
        requestTimeout: null,
        grpcDeadline: null,
        minBackoff: 250,
        maxBackoff: 8000,
        logger: null,
        ...overrides,
    });
}

describe("RetryExecutor", function () {
    it("_getExecutionNode returns the network node and seeds nodeAccountIds when empty", function () {
        const executor = createRetryExecutor();
        const nodeAccountIds = new List();
        const currentNode = { accountId: new AccountId(3) };
        const client = {
            _network: {
                getNode: () => currentNode,
            },
        };

        const result = executor._getExecutionNode(nodeAccountIds, client);

        expect(result).to.equal(currentNode);
        expect(nodeAccountIds.current.toString()).to.equal("0.0.3");
    });

    it("_getExecutionNode returns the resolved network node when nodeAccountIds are already set", function () {
        const executor = createRetryExecutor();
        const nodeAccountIds = new List();
        nodeAccountIds.setList([new AccountId(111)]);
        const currentNode = { accountId: new AccountId(3) };
        const client = {
            _network: {
                getNode: () => currentNode,
            },
        };

        const result = executor._getExecutionNode(nodeAccountIds, client);

        expect(result).to.equal(currentNode);
        expect(nodeAccountIds.current.toString()).to.equal("0.0.111");
    });

    it("_handleUnhealthyNode advances to the next node when failover is possible", async function () {
        const debug = vi.fn();
        const executor = createRetryExecutor({
            logger: { debug },
        });
        const nodeAccountIds = new List();
        nodeAccountIds.setList([new AccountId(3), new AccountId(4)]);

        const strategy = { _getLogId: () => "debug.log" };
        const client = { isLocalNetwork: false };

        await executor._handleUnhealthyNode(
            strategy,
            { isHealthy: () => false },
            {},
            1,
            nodeAccountIds,
            client,
        );

        expect(nodeAccountIds.current.toString()).to.equal("0.0.4");
        expect(debug).toHaveBeenCalledWith(
            "[debug.log] Node is not healthy, trying the next node.",
        );
    });

    it("_handleInvalidNodeAccountId marks the node unusable and warns when no mirror network is configured", async function () {
        const debug = vi.fn();
        const trace = vi.fn();
        const warn = vi.fn();
        const executor = createRetryExecutor({
            logger: { debug, warn, trace },
        });
        const increaseBackoff = vi.fn();
        const updateNetwork = vi.fn();
        const currentNode = {
            address: { toString: () => "127.0.0.1:50211" },
        };
        const client = {
            _network: { increaseBackoff },
            mirrorNetwork: [],
            updateNetwork,
        };

        const strategy = { _getLogId: () => "logger.id" };

        await executor._handleInvalidNodeAccountId(
            strategy,
            client,
            currentNode,
            new AccountId(3),
        );

        expect(increaseBackoff).toHaveBeenCalledWith(currentNode);
        expect(updateNetwork).not.toHaveBeenCalled();
        expect(warn).toHaveBeenCalledWith(
            "[logger.id] Cannot update address book: no mirror network configured. Retrying with existing network configuration.",
        );
    });

    it("_shouldRetryRequestError is driven by retryable error type and attempt count", function () {
        const executor = createRetryExecutor({ maxAttempts: 3 });
        const strategy = {};
        const error = new GrpcServiceError(GrpcStatus.Unavailable);

        expect(executor._shouldRetryRequestError(strategy, error, 1)).to.be
            .true;
        expect(executor._shouldRetryRequestError(strategy, error, 4)).to.be
            .false;
        expect(
            executor._shouldRetryRequestError(strategy, new Error("nope"), 1),
        ).to.be.false;
    });

    it("_executeRequestWithGrpcDeadline returns the execution response and clears the deadline timer", async function () {
        const trace = vi.fn();
        const executor = createRetryExecutor({
            grpcDeadline: 1000,
            logger: {
                trace,
                debug() {},
                info() {},
                warn() {},
                error() {},
                fatal() {},
            },
        });
        const response = { ok: true };
        const deadlineTimer = { id: "deadline" };
        const setTimeoutSpy = vi
            .spyOn(globalThis, "setTimeout")
            .mockImplementation(() => deadlineTimer);
        const clearTimeoutSpy = vi
            .spyOn(globalThis, "clearTimeout")
            .mockImplementation(() => {});

        const strategy = {
            _getLogId: () => "trace.log",
            _requestToBytes: () => new Uint8Array([1, 2, 3]),
            _execute: vi.fn().mockResolvedValue(response),
        };

        try {
            const result = await executor._executeRequestWithGrpcDeadline(
                strategy,
                {},
                { request: true },
            );

            expect(result).to.equal(response);
            expect(trace).toHaveBeenCalledWith(
                "[trace.log] sending protobuf 010203",
            );
            expect(setTimeoutSpy).toHaveBeenCalledOnce();
            expect(clearTimeoutSpy).toHaveBeenCalledWith(deadlineTimer);
        } finally {
            setTimeoutSpy.mockRestore();
            clearTimeoutSpy.mockRestore();
        }
    });
});
