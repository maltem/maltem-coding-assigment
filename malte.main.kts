// I run this program like this:
//
//   kotlinc -script malte.main.kts

@file:DependsOn("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.4.2")

import java.util.Arrays
import kotlinx.coroutines.*

runBlocking {
    println("\nTesting round-robin load balancer. Number of providers: 7. Heart beat: every 2 seconds.")
    test(LoadBalancer.makeRoundRobin(7), 2000L)
}
runBlocking {
    println("\nTesting random load balancer. Number of providers: 7. Heart beat: every 2 seconds.")
    test(LoadBalancer.makeRandom(7), 2000L)
}

fun test(lb : LoadBalancer, heartBeatInterval : Long) {
    val heartBeatChecker = GlobalScope.launch {
        while (true) {
            lb.heartBeat()
            delay(heartBeatInterval)
        }
    }
    // Test the load balancer ten times in a row,  in intervals of 1 second
    // (just so that we have something to look at and can see the effect of
    // the heart-beat check).
    for (i in 0 until 10) {
        // Let's say there are 11 requests coming in right now.
        // Ask for an assignment of these 11 requests:
        try {
            val assignment = lb.assign(11)
            println("Assignments for 11 incoming requests: " + Arrays.toString(assignment))
        } catch (ex: Exception) {
            println("Service temporarily not available, waiting for providers.")
        }
        Thread.sleep(1000L)
    }
    heartBeatChecker.cancel()
}

// The way I interpret Step 1 of the coding assignment, I am supposed to write
// a Provider class with a method get(); here it is.
class Provider(val id : String) {

    fun get() = id

    // Returns true if the provider is alive. In lack of a real network to
    // test, we just have a provider fail with 20 % probability any time it is
    // checked.
    fun check() : Boolean =
        (0 until 10).random() < 8
}

// The load balancer stores a ProviderState for each Provider. Once a Provider
// gets unavailable, the load balancer marks it as `disabled`. When the
// Provider comes back alive, it is successively marked as `ready` and
// `enabled`.
enum class ProviderState {
    disabled, ready, enabled;

    fun next() : ProviderState =
        when (this) {
            disabled -> ready
            else     -> enabled
        }
}

abstract class LoadBalancer(nProviders : Int) {
    val providers : Array<Provider> =
        Array(nProviders) { i -> Provider(i.toString()) }
    val states : Array<ProviderState> =
        Array(nProviders) { _ -> ProviderState.enabled }
    // `nEnabled` caches the number of entries in `states` equal to
    // `ProviderState.enabled`.
    var nEnabled = nProviders

    companion object {
        // Being new to Kotlin, a factory function was the only obvious way I
        // found to enforce the limit of 10 providers (Step 2 of the coding
        // assignment)
        fun makeRoundRobin(nProviders : Int) : RoundRobin {
            require (nProviders >= 0)
            require (nProviders <= 10)
            return LoadBalancer.RoundRobin(nProviders)
        }

        fun makeRandom(nProviders : Int) : Random {
            require (nProviders >= 0)
            require (nProviders <= 10)
            return LoadBalancer.Random(nProviders)
        }
    }

    // Given `nTasks` many incoming requests, returns an array of length
    // `nTasks`. Entry i (where i = 0 until nTasks) is the identifier of the
    // Provider being assigned the ith incoming request.
    fun assign(nTasks : Int) : Array<String> {
        require (nTasks > 0)
        require (nTasks <= 3*nEnabled)
            // ^^^ This is how I ended up interpreting Step 8 of the coding
            // assignment. Not sure if it was inteded like this or if I should
            // keep track of how many requests are currently being processed.
        return Array(nTasks) { _ -> get()!! }
    }

    // Implementation of Step 6 of the coding assignment
    fun heartBeat() {
        for (i in 0 until providers.size) {
            if ( providers[i].check() ) {
                if (states[i] == ProviderState.ready) ++nEnabled
                states[i] = states[i].next()
            } else {
              if (states[i] == ProviderState.enabled) --nEnabled
              states[i] = ProviderState.disabled
            }
        }
    }

    abstract fun get() : String?

    class Random(n : Int) : LoadBalancer(n) {
        // Returns the identifier of a Provider chosen uniformly at random
        // from those Providers that have state equal to ProviderState.enabled.
        // Returns null if there are no such Providers available.
        override fun get() : String? {
            if (nEnabled == 0) return null;
            var i : Int;
            do {
                i = (0 until providers.size).random()
            } while (states[i] != ProviderState.enabled)
            return providers[i].get()
        }
    }

    class RoundRobin(n : Int) : LoadBalancer(n) {
        // Index of the Provider that was returned on the most recent
        // invocation of get()
        var cursor = -1

        override fun get() : String? {
            if (nEnabled == 0) return null;
            do {
                cursor = (cursor + 1) % providers.size
            } while (states[cursor] != ProviderState.enabled)
            return providers[cursor].get()
        }
    }
}
