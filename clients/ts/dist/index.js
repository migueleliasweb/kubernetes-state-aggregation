import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { StateService } from "./gen/v1/state_pb.js";
export * from "./gen/v1/state_pb.js";
/**
 * Creates a type-safe KSA StateService client for browser or Node.js environments.
 */
export function createKSAClient(optionsOrTransport) {
    let transport;
    if (typeof optionsOrTransport === "string") {
        transport = createConnectTransport({
            baseUrl: optionsOrTransport,
        });
    }
    else if ("baseUrl" in optionsOrTransport) {
        transport = createConnectTransport({
            baseUrl: optionsOrTransport.baseUrl,
            useBinaryFormat: optionsOrTransport.useBinaryFormat ?? false,
        });
    }
    else {
        transport = optionsOrTransport;
    }
    return createClient(StateService, transport);
}
//# sourceMappingURL=index.js.map