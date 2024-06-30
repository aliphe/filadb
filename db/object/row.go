package object

type ID string

type Row map[string]interface{}

func (r Row) ObjectID() ID {
	// TODO check this
	id, _ := r["id"].(string)
	return ID(id)
}

func (r Row) ObjectTable() Table {
	// TODO check this
	table, _ := r["table"].(string)
	return Table(table)
}

type Table string
