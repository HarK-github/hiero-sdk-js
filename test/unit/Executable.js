import Executable, { RST_STREAM } from "../../src/Executable.js";
import AccountId from "../../src/account/AccountId.js";

describe("Executable", function () {
    it("RST_STREAM regex matches actual response returned", function () {
        expect(
            RST_STREAM.test(
                "Error: 13 INTERNAL: Received RST_STREAM with code 0",
            ),
        ).to.be.true;
    });

    it("_validateTransactionNodeIds throws when transaction nodes are not in the client node list", function () {
        const executable = new Executable();

        executable._nodeAccountIds.setList([
            new AccountId(3),
            new AccountId(4),
        ]);
        executable.transactionNodeIds = ["0.0.111"];

        expect(() => executable._validateTransactionNodeIds()).to.throw(
            "Attempting to execute a transaction against nodes 0.0.3, 0.0.4, which are not included in the Client's node list. Please review your Client configuration.",
        );
    });

    it("_setupExecution initializes execution settings from the client", async function () {
        const executable = new Executable();
        const logger = { trace() {}, debug() {}, warn() {} };
        let beforeExecuteCalled = false;

        executable._beforeExecute = async () => {
            beforeExecuteCalled = true;
        };

        await executable._setupExecution(
            {
                _logger: logger,
                requestTimeout: 15000,
                grpcDeadline: 5000,
                maxBackoff: 8000,
                minBackoff: 250,
                maxAttempts: 10,
            },
            12000,
        );

        expect(executable.logger).to.equal(logger);
        expect(executable._requestTimeout).to.equal(12000);
        expect(executable.grpcDeadline).to.equal(5000);
        expect(executable.maxBackoff).to.equal(8000);
        expect(executable.minBackoff).to.equal(250);
        expect(executable.maxAttempts).to.equal(10);
        expect(beforeExecuteCalled).to.be.true;
    });

    it("_shouldSkipAttemptForNodeAccountId returns true when the node is not in transactionNodeIds", function () {
        const executable = new Executable();

        executable._nodeAccountIds.setList([
            new AccountId(3),
            new AccountId(4),
        ]);
        executable.transactionNodeIds = ["0.0.111"];

        const shouldSkip = executable._shouldSkipAttemptForNodeAccountId(
            new AccountId(3),
        );

        expect(shouldSkip).to.be.true;
    });

    it("_shouldSkipAttemptForNodeAccountId returns false when the node is in transactionNodeIds", function () {
        const executable = new Executable();

        executable._nodeAccountIds.setList([new AccountId(3)]);
        executable.transactionNodeIds = ["0.0.3"];

        const shouldSkip = executable._shouldSkipAttemptForNodeAccountId(
            new AccountId(3),
        );

        expect(shouldSkip).to.be.false;
    });
});
