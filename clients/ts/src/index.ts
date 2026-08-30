import { createClient, type Client, type Transport } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { StateService } from "./gen/v1/state_pb.js";

export * from "./gen/v1/state_pb.js";

export interface KSAClientOptions {
  baseUrl: string;
  useBinaryFormat?: boolean;
}

/**
 * Creates a type-safe KSA StateService client for browser or Node.js environments.
 */
export function createKSAClient(
  optionsOrTransport: string | KSAClientOptions | Transport,
): Client<typeof StateService> {
  let transport: Transport;

  if (typeof optionsOrTransport === "string") {
    transport = createConnectTransport({
      baseUrl: optionsOrTransport,
    });
  } else if ("baseUrl" in optionsOrTransport) {
    transport = createConnectTransport({
      baseUrl: optionsOrTransport.baseUrl,
      useBinaryFormat: optionsOrTransport.useBinaryFormat ?? false,
    });
  } else {
    transport = optionsOrTransport;
  }

  return createClient(StateService, transport);
}

export type KSAClient = Client<typeof StateService>;
