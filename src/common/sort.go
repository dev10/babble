package common

type IntSlice []int

func (a IntSlice) Len() int      { return len(a) }
func (a IntSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a IntSlice) Less(i, j int) bool {
	return a[i] < a[j]
}