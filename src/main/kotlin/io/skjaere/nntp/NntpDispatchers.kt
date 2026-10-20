package io.skjaere.nntp

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers

/** Where the library's connections wait for their servers. */
object NntpDispatchers {
    /**
     * How many waits may hold a thread at once, across all pools: far more than a provider's connections. One more
     * waits its turn.
     */
    const val PARALLELISM = 512

    /**
     * The library's view of Dispatchers.IO. A read with nothing to take waits in its thread ([NntpSocket]), as does a
     * connect and a write the server is slow to take: on Dispatchers.IO each held one of the 64 threads it has for the
     * whole process (kotlinx.coroutines.io.parallelism), so a stalled provider or a wave of logins with that many
     * connections left every other IO task waiting behind them. A view of IO is not counted in IO's limit: its threads
     * are IO's, made as they are needed, up to [PARALLELISM].
     *
     * A caller on it reads, writes and connects in place, with no hand-off; one elsewhere, Dispatchers.IO included,
     * moves to it for each of those: network work that reads a lot runs best on it.
     */
    val IO: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(PARALLELISM, "NntpDispatchers.IO")
}
