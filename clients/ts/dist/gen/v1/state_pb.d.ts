import type { GenFile, GenMessage, GenService } from "@bufbuild/protobuf/codegenv2";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import type { Message } from "@bufbuild/protobuf";
/**
 * Describes the file v1/state.proto.
 */
export declare const file_v1_state: GenFile;
/**
 * ResourceRecord represents a serialized Kubernetes resource state.
 *
 * @generated from message ksa.v1.ResourceRecord
 */
export type ResourceRecord = Message<"ksa.v1.ResourceRecord"> & {
    /**
     * @generated from field: string uid = 1;
     */
    uid: string;
    /**
     * @generated from field: string cluster_name = 2;
     */
    clusterName: string;
    /**
     * @generated from field: string group_name = 3;
     */
    groupName: string;
    /**
     * @generated from field: string version = 4;
     */
    version: string;
    /**
     * @generated from field: string kind = 5;
     */
    kind: string;
    /**
     * @generated from field: string namespace = 6;
     */
    namespace: string;
    /**
     * @generated from field: string name = 7;
     */
    name: string;
    /**
     * @generated from field: string resource_version = 8;
     */
    resourceVersion: string;
    /**
     * @generated from field: bytes manifest = 9;
     */
    manifest: Uint8Array;
    /**
     * @generated from field: bytes labels = 10;
     */
    labels: Uint8Array;
    /**
     * @generated from field: bytes annotations = 11;
     */
    annotations: Uint8Array;
    /**
     * @generated from field: google.protobuf.Timestamp updated_at = 12;
     */
    updatedAt?: Timestamp | undefined;
};
/**
 * Describes the message ksa.v1.ResourceRecord.
 * Use `create(ResourceRecordSchema)` to create a new message.
 */
export declare const ResourceRecordSchema: GenMessage<ResourceRecord>;
/**
 * ResourceInfo defines the base information to identify a resource.
 *
 * @generated from message ksa.v1.ResourceInfo
 */
export type ResourceInfo = Message<"ksa.v1.ResourceInfo"> & {
    /**
     * @generated from field: string cluster_name = 1;
     */
    clusterName: string;
    /**
     * @generated from field: string group = 2;
     */
    group: string;
    /**
     * @generated from field: string version = 3;
     */
    version: string;
    /**
     * @generated from field: string kind = 4;
     */
    kind: string;
    /**
     * @generated from field: string namespace = 5;
     */
    namespace: string;
    /**
     * @generated from field: string name = 6;
     */
    name: string;
    /**
     * @generated from field: string uid = 7;
     */
    uid: string;
    /**
     * @generated from field: string resource_version = 8;
     */
    resourceVersion: string;
};
/**
 * Describes the message ksa.v1.ResourceInfo.
 * Use `create(ResourceInfoSchema)` to create a new message.
 */
export declare const ResourceInfoSchema: GenMessage<ResourceInfo>;
/**
 * FetchResourceGraphRequest requests the dependency graph for a root resource.
 *
 * @generated from message ksa.v1.FetchResourceGraphRequest
 */
export type FetchResourceGraphRequest = Message<"ksa.v1.FetchResourceGraphRequest"> & {
    /**
     * @generated from field: ksa.v1.ResourceInfo root = 1;
     */
    root?: ResourceInfo | undefined;
};
/**
 * Describes the message ksa.v1.FetchResourceGraphRequest.
 * Use `create(FetchResourceGraphRequestSchema)` to create a new message.
 */
export declare const FetchResourceGraphRequestSchema: GenMessage<FetchResourceGraphRequest>;
/**
 * FetchResourceGraphResponse returns the list of unique resources in the graph.
 *
 * @generated from message ksa.v1.FetchResourceGraphResponse
 */
export type FetchResourceGraphResponse = Message<"ksa.v1.FetchResourceGraphResponse"> & {
    /**
     * @generated from field: repeated ksa.v1.ResourceRecord items = 1;
     */
    items: ResourceRecord[];
};
/**
 * Describes the message ksa.v1.FetchResourceGraphResponse.
 * Use `create(FetchResourceGraphResponseSchema)` to create a new message.
 */
export declare const FetchResourceGraphResponseSchema: GenMessage<FetchResourceGraphResponse>;
/**
 * GetResourceRequest requests a single resource by its info/identifiers.
 *
 * @generated from message ksa.v1.GetResourceRequest
 */
export type GetResourceRequest = Message<"ksa.v1.GetResourceRequest"> & {
    /**
     * @generated from field: ksa.v1.ResourceInfo info = 1;
     */
    info?: ResourceInfo | undefined;
};
/**
 * Describes the message ksa.v1.GetResourceRequest.
 * Use `create(GetResourceRequestSchema)` to create a new message.
 */
export declare const GetResourceRequestSchema: GenMessage<GetResourceRequest>;
/**
 * GetResourceResponse returns the requested resource record.
 *
 * @generated from message ksa.v1.GetResourceResponse
 */
export type GetResourceResponse = Message<"ksa.v1.GetResourceResponse"> & {
    /**
     * @generated from field: ksa.v1.ResourceRecord record = 1;
     */
    record?: ResourceRecord | undefined;
};
/**
 * Describes the message ksa.v1.GetResourceResponse.
 * Use `create(GetResourceResponseSchema)` to create a new message.
 */
export declare const GetResourceResponseSchema: GenMessage<GetResourceResponse>;
/**
 * ListResourcesRequest requests a filtered list of resources.
 *
 * @generated from message ksa.v1.ListResourcesRequest
 */
export type ListResourcesRequest = Message<"ksa.v1.ListResourcesRequest"> & {
    /**
     * @generated from field: ksa.v1.ResourceInfo filter = 1;
     */
    filter?: ResourceInfo | undefined;
};
/**
 * Describes the message ksa.v1.ListResourcesRequest.
 * Use `create(ListResourcesRequestSchema)` to create a new message.
 */
export declare const ListResourcesRequestSchema: GenMessage<ListResourcesRequest>;
/**
 * ListResourcesResponse returns the matching resource records.
 *
 * @generated from message ksa.v1.ListResourcesResponse
 */
export type ListResourcesResponse = Message<"ksa.v1.ListResourcesResponse"> & {
    /**
     * @generated from field: repeated ksa.v1.ResourceRecord items = 1;
     */
    items: ResourceRecord[];
};
/**
 * Describes the message ksa.v1.ListResourcesResponse.
 * Use `create(ListResourcesResponseSchema)` to create a new message.
 */
export declare const ListResourcesResponseSchema: GenMessage<ListResourcesResponse>;
/**
 * StateService provides access to aggregated Kubernetes state.
 *
 * @generated from service ksa.v1.StateService
 */
export declare const StateService: GenService<{
    /**
     * @generated from rpc ksa.v1.StateService.FetchResourceGraph
     */
    fetchResourceGraph: {
        methodKind: "unary";
        input: typeof FetchResourceGraphRequestSchema;
        output: typeof FetchResourceGraphResponseSchema;
    };
    /**
     * @generated from rpc ksa.v1.StateService.GetResource
     */
    getResource: {
        methodKind: "unary";
        input: typeof GetResourceRequestSchema;
        output: typeof GetResourceResponseSchema;
    };
    /**
     * @generated from rpc ksa.v1.StateService.ListResources
     */
    listResources: {
        methodKind: "unary";
        input: typeof ListResourcesRequestSchema;
        output: typeof ListResourcesResponseSchema;
    };
}>;
//# sourceMappingURL=state_pb.d.ts.map