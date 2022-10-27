import org.junit.Assert
import org.junit.Test

internal class uzajTests {
    @Test
    fun testInterests() {
        val i = parser.parseDocument("query Foo { bar @demo }").interests
        Assert.assertEquals(1, i.directives.size)
        Assert.assertEquals("@demo", i.directives.first())
    }
}
