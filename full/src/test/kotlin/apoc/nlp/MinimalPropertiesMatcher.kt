package apoc.nlp

import org.hamcrest.Description

data class MinimalPropertiesMatcher(private val properties: Map<String, Any>) : org.hamcrest.TypeSafeDiagnosingMatcher<Map<String, Any>>() {
    override fun describeTo(description: Description?) {
        description?.appendText(" a map containing ")?.appendValue(properties)
    }

    override fun matchesSafely(item: Map<String, Any>, mismatchDescription: Description?): Boolean {
        val propertiesMatch =  properties!!.all { entry -> item?.containsKey(entry.key)!! && item[entry.key] == entry.value }
        if(!propertiesMatch) {
            mismatchDescription!!.appendText(",  properties:").appendValue(item)
            return false
        }
        return true
    }

    companion object {
        fun hasAtLeast(properties: Map<String, Any>): MinimalPropertiesMatcher = MinimalPropertiesMatcher(properties)
    }
}