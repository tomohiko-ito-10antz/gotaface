package truncate

type Clearer interface {
	Clear(table string) error
}
