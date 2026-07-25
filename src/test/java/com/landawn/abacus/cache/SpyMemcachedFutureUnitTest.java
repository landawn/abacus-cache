package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Service-free regression coverage for the bounded bulk-future adapter. */
@Tag("2025")
public class SpyMemcachedFutureUnitTest {

    @Test
    @SuppressWarnings("unchecked")
    public void noArgumentGetIsBoundedWhileExplicitTimeoutPassesThrough() throws Exception {
        final Class<?> adapterType = Class.forName(SpyMemcached.class.getName() + "$DefaultTimeoutFuture");
        final Constructor<?> constructor = adapterType.getDeclaredConstructor(Future.class, long.class);
        constructor.setAccessible(true);

        final Future<String> timeoutDelegate = mock(Future.class);
        when(timeoutDelegate.get(25L, TimeUnit.MILLISECONDS)).thenThrow(new TimeoutException("test timeout"));
        final Future<String> bounded = (Future<String>) constructor.newInstance(timeoutDelegate, 25L);

        final ExecutionException error = assertThrows(ExecutionException.class, bounded::get);
        assertTrue(error.getCause() instanceof TimeoutException);
        verify(timeoutDelegate).get(25L, TimeUnit.MILLISECONDS);
        verify(timeoutDelegate).cancel(true);

        final Future<String> timedDelegate = mock(Future.class);
        when(timedDelegate.get(7L, TimeUnit.SECONDS)).thenReturn("value");
        final Future<String> explicitlyTimed = (Future<String>) constructor.newInstance(timedDelegate, 25L);
        assertEquals("value", explicitlyTimed.get(7L, TimeUnit.SECONDS));
        verify(timedDelegate).get(7L, TimeUnit.SECONDS);
    }
}
