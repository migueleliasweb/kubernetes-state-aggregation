import { MethodKind } from "@bufbuild/protobuf";
/**
 * StateService provides access to aggregated Kubernetes state.
 *
 * @generated from service ksa.v1.StateService
 */
export declare const StateService: {
    readonly typeName: "ksa.v1.StateService";
    readonly methods: {
        /**
         * @generated from rpc ksa.v1.StateService.FetchResourceGraph
         */
        readonly fetchResourceGraph: {
            readonly name: "FetchResourceGraph";
            readonly I: any;
            readonly O: any;
            readonly kind: MethodKind.Unary;
        };
        /**
         * @generated from rpc ksa.v1.StateService.GetResource
         */
        readonly getResource: {
            readonly name: "GetResource";
            readonly I: any;
            readonly O: any;
            readonly kind: MethodKind.Unary;
        };
        /**
         * @generated from rpc ksa.v1.StateService.ListResources
         */
        readonly listResources: {
            readonly name: "ListResources";
            readonly I: any;
            readonly O: any;
            readonly kind: MethodKind.Unary;
        };
    };
};
//# sourceMappingURL=state_connect.d.ts.map