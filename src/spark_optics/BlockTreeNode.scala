package spark_optics

/**
  * Created by Administrator on 2016/6/24.
  */
class BlockTreeNode(var block: Block, var leftChildren: BlockTreeNode = null
                    , var rightChildren: BlockTreeNode = null) {
}
