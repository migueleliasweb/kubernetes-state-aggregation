import { type Client, type Transport } from "@connectrpc/connect";
import { StateService } from "./gen/v1/state_pb.js";
export * from "./gen/v1/state_pb.js";
export interface KSAClientOptions {
    baseUrl: string;
    useBinaryFormat?: boolean;
}
/**
 * Creates a type-safe KSA StateService client for browser or Node.js environments.
 */
export declare function createKSAClient(optionsOrTransport: string | KSAClientOptions | Transport): Client<typeof StateService>;
export type KSAClient = Client<typeof StateService>;
//# sourceMappingURL=index.d.ts.map