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
	internalTableTables  InternalTable = "tables"
	internalTableColumns InternalTable = "columns"
)

var internalTableTablesSchema = Schema{
	Table: string(internalTableTables),
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
	Table: string(internalTableColumns),
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
