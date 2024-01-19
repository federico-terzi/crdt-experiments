class CRDTAddOnlySet {
    constructor() {
        this.set = new Set();
    }

    add(element) {
        this.set.add(element);
    }

    has(element) {
        return this.set.has(element);
    }

    merge(otherSet) {
        for (let element of otherSet.set) {
            this.set.add(element);
        }
    }
}

const replicaA = new CRDTAddOnlySet();
replicaA.add('a');
console.log(replicaA.has('a')); // true
console.log(replicaA.has('b')); // false

const replicaB = new CRDTAddOnlySet();
replicaB.add('b');
console.log(replicaB.has('a')); // false
console.log(replicaB.has('b')); // true

replicaA.merge(replicaB);
console.log(replicaA.has('a')); // true
console.log(replicaA.has('b')); // true