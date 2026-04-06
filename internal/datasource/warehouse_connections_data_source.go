package datasource

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/velodb/terraform-provider-velodb/internal/client"
)

var _ datasource.DataSource = &WarehouseConnectionsDataSource{}

type WarehouseConnectionsDataSource struct {
	client *client.FormationClient
}

func NewWarehouseConnectionsDataSource() datasource.DataSource {
	return &WarehouseConnectionsDataSource{}
}

type WarehouseConnectionsDataSourceModel struct {
	WarehouseID types.String `tfsdk:"warehouse_id"`
	Clusters    types.List   `tfsdk:"clusters"`
}

func (d *WarehouseConnectionsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_warehouse_connections"
}

func (d *WarehouseConnectionsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Get connection endpoints for all clusters in a VeloDB warehouse.",
		Attributes: map[string]schema.Attribute{
			"warehouse_id": schema.StringAttribute{
				Description: "Warehouse identifier.",
				Required:    true,
			},
			"clusters": schema.ListNestedAttribute{
				Description: "Connection information per cluster.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"cluster_id":          schema.StringAttribute{Computed: true},
						"type":                schema.StringAttribute{Computed: true},
						"jdbc_port":           schema.Int64Attribute{Computed: true},
						"http_port":           schema.Int64Attribute{Computed: true},
						"stream_load_port":    schema.Int64Attribute{Computed: true},
						"public_endpoint":     schema.StringAttribute{Computed: true},
						"private_endpoint":    schema.StringAttribute{Computed: true},
						"listener_port":       schema.Int64Attribute{Computed: true},
						"endpoint_service_id": schema.StringAttribute{Computed: true},
					},
				},
			},
		},
	}
}

func (d *WarehouseConnectionsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	c, ok := req.ProviderData.(*client.FormationClient)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data type", fmt.Sprintf("Expected *client.FormationClient, got: %T", req.ProviderData))
		return
	}
	d.client = c
}

func (d *WarehouseConnectionsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config WarehouseConnectionsDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conns, err := d.client.GetWarehouseConnections(ctx, config.WarehouseID.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Error reading warehouse connections", err.Error())
		return
	}

	connAttrTypes := map[string]attr.Type{
		"cluster_id":          types.StringType,
		"type":                types.StringType,
		"jdbc_port":           types.Int64Type,
		"http_port":           types.Int64Type,
		"stream_load_port":    types.Int64Type,
		"public_endpoint":     types.StringType,
		"private_endpoint":    types.StringType,
		"listener_port":       types.Int64Type,
		"endpoint_service_id": types.StringType,
	}

	var items []attr.Value
	for _, c := range conns.Clusters {
		obj, diags := types.ObjectValue(connAttrTypes, map[string]attr.Value{
			"cluster_id":          types.StringValue(c.ClusterID),
			"type":                types.StringValue(c.Type),
			"jdbc_port":           types.Int64Value(int64(c.JdbcPort)),
			"http_port":           types.Int64Value(int64(c.HttpPort)),
			"stream_load_port":    types.Int64Value(int64(c.StreamLoadPort)),
			"public_endpoint":     types.StringValue(c.PublicEndpoint),
			"private_endpoint":    types.StringValue(c.PrivateEndpoint),
			"listener_port":       types.Int64Value(int64(c.ListenerPort)),
			"endpoint_service_id": stringVal(c.EndpointServiceID),
		})
		resp.Diagnostics.Append(diags...)
		items = append(items, obj)
	}

	list, diags := types.ListValue(types.ObjectType{AttrTypes: connAttrTypes}, items)
	resp.Diagnostics.Append(diags...)

	config.Clusters = list
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}
