package rsm

type IdGenerator interface {
	NextID() int64
}
