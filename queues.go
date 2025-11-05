package mr

type MappingQueueNode struct {
	inputFilePath, outputFilePath string
	next                          *MappingQueueNode
	prev                          *MappingQueueNode
}
type ReduceQueueNode struct {
	inputFilePath, outputFilePath string
	next                          *ReduceQueueNode
	prev                          *ReduceQueueNode
}

func (c *Coordinator) IsMapQueueEmpty() bool {
	if c.MapQueueHead == nil {
		return true
	} else {
		return false
	}
}

func NewMapTask(inPath, outPath string) *MappingQueueNode {
	newInstance := MappingQueueNode{inPath, outPath, nil, nil}
	return &newInstance
}

func (c *Coordinator) InsertIntoMapQueue(node *MappingQueueNode) {
	if c.MapQueueHead == nil {
		c.MapQueueHead = node
		return
	}
	node.next = c.MapQueueHead
	c.MapQueueHead.prev = node
	c.MapQueueHead = node
}

func (c *Coordinator) PopMapTask() *MappingQueueNode {
	tmp := c.MapQueueHead
	for ; tmp.next != nil; tmp = tmp.next {
	}
	back := tmp.prev
	back.next = nil
	tmp.prev = nil
	return tmp
}

func NewReduceTask(inPath, outPath string) *ReduceQueueNode {
	newInstance := ReduceQueueNode{inPath, outPath, nil, nil}
	return &newInstance
}

func (c *Coordinator) InsertIntoReduceQueue(node *ReduceQueueNode) {
	NewNode := NewReduceTask(node.inputFilePath, node.outputFilePath)

	if c.reduceQueueHead == nil {
		c.reduceQueueHead = NewNode
		return
	}
	NewNode.next = c.reduceQueueHead
	c.reduceQueueHead.prev = NewNode
	c.reduceQueueHead = NewNode
}

func (c *Coordinator) PopReduceTask() *ReduceQueueNode {
	tmp := c.reduceQueueHead
	for ; tmp.next != nil; tmp = tmp.next {
	}
	back := tmp.prev
	back.next = nil
	tmp.prev = nil
	return tmp
}
