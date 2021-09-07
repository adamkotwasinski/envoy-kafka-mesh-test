package example;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;

public class ThingUnitTest {

    @Test
    public void shouldReturnNumber() {
        // given
        final Thing thing = new Thing();

        // when
        final int result = thing.makeNumber();

        // then
        assertThat(result, equalTo(42));
    }

}
