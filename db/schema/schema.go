package schema

type Schema struct {
	version int32
	Table   string
	Columns []Column
}

type Column struct {
	Name string
	Type ColumnType
}

type ColumnType string

const (
	ColumnTypeText   ColumnType = "text"
	ColumnTypeNumber ColumnType = "number"
)

type InternalTable string

const (
	InternalTableTables  InternalTable = "tables"
	InternalTableColumns InternalTable = "columns"
)

var internalTableTablesSchema = Schema{
	Table: string(InternalTableColumns),
	Columns: []Column{
		{
			Name: "table",
			Type: ColumnTypeText,
		},
		{
			Name: "version",
			Type: ColumnTypeNumber,
		},
	},
}

var internalTableColumnsSchema = Schema{
	Table: string(InternalTableColumns),
	Columns: []Column{
		{
			Name: "table",
			Type: ColumnTypeText,
		},
		{
			Name: "column",
			Type: ColumnTypeText,
		},
		{
			Name: "type",
			Type: ColumnTypeText,
		},
	},
}
