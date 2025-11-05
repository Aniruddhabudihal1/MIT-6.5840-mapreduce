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

func NewMapTask(inPath, outPath string) *MappingQueueNode {
	newInstance := MappingQueueNode{inPath, outPath, nil, nil}
	return &newInstance
}

func (c *Coordinator) InsertIntoMapQueue(node *MappingQueueNode) {
	NewNode := NewMapTask(node.inputFilePath, node.outputFilePath)

	if c.MapQueueHead == nil {
		c.MapQueueHead = NewNode
		return
	}
	NewNode.next = c.MapQueueHead
	c.MapQueueHead.prev = NewNode
	c.MapQueueHead = NewNode
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
