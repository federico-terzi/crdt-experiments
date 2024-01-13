# CRDT Experiments

This repository contains several experiments related to [CRDTs (_Conflict-free Replicated Data Types_)](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type), a class of data structures that allows multiple peers to seamlessly synchronize across arbitrary network topologies (eg. Google Docs-like synchronization, P2P file synchronization, and more)

* [`json-crdt-rust`](/json-crdt-rust) - An experimental, high-performance JSON CRDT written in Rust

Note: none of these projects is production-ready, nor it's feature-complete. They could be helpful to grasp these concepts, but I'd not recommend using them in production as is. Other alternatives like [Yjs](https://github.com/yjs/yjs) and [Automerge](https://github.com/automerge/automerge) are more suited for production use.

## Why did you do this?

Lately, I've been fascinated by distributed systems, and in particular multi-master, P2P networks.
While researching more on the topic, I stumbled upon the concept of CRDTs, a kind of data structure that promised a robust approach to P2P synchronization, even when dealing with intermittent internet connections and complex network topologies.

While the basic concepts are not too difficult to grasp, building an efficient CRDT that supports real-world use cases like real-time text editing (eg. Google Docs) is not trivial due to the data structures and optimizations involved.

For me, the best way to understand a complex algorithm is to implement it from scratch, so... here it is :)

## Where can I find out more?

I'm preparing a series of articles to explain CRDTs from the ground up, starting from the theory and going through several possible implementations and optimizations that real-world CRDTs use.

If you are interested, either activate the notifications on this repository, or follow my blog at: https://federicoterzi.com/blog :)
