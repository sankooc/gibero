package gibero

const (
	TYPE_FORWARD_ONLY       uint32 = 1003
	TYPE_SCROLL_INSENSITIVE        = 1004
	TYPE_SCROLL_SENSITIVE          = 1005
	CONCUR_READ_ONLY               = 1007
	CONCUR_UPDATABLE               = 1008
)

type RsetType struct {
	rank        int
	rtype       int
	concurrency int
}

// const (
// RTNULL *RsetType = RsetType{0, -1, -1}
// FWRD             = &RsetType{1, 1003, 1007}
// FWUP
// SIRD
// SIUP
// SSRD
// SSUP
// )

var RTTNULL = &RsetType{0, -1, -1}
var RTFWRD = &RsetType{1, 1003, 1007}
var RTFWUP = &RsetType{2, 1003, 1008}
var RTSIRD = &RsetType{3, 1004, 1007}
var RTSIUP = &RsetType{4, 1004, 1008}
var RTSSRD = &RsetType{5, 1005, 1008}
var RTSSUP = &RsetType{6, 1005, 1007}

var TB_VERBOSE = false
