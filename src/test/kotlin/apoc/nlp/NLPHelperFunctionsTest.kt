package apoc.nlp

import apoc.result.VirtualNode
import org.junit.Assert
import org.junit.Test

class NLPHelperFunctionsTest {
    @Test
    fun `should partition sources`() {
        Assert.assertEquals(
                listOf(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3)), listOf(VirtualNode(4))),
                NLPHelperFunctions.partition(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3), VirtualNode(4)), 3)
        )

        Assert.assertEquals(
                listOf(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3))),
                NLPHelperFunctions.partition(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3)), 3)
        )

        Assert.assertEquals(
                listOf(listOf(VirtualNode(1)), listOf(VirtualNode(2)), listOf(VirtualNode(3))),
                NLPHelperFunctions.partition(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3)), 1)
        )
    }
}
