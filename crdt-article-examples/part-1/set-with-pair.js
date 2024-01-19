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

class CRDTSet {
    constructor() {
        this.elements = new CRDTAddOnlySet();
        this.removals = new CRDTAddOnlySet();
    }

    add(element) {
        this.elements.add(element);
    }

    has(element) {
        if (this.removals.has(element)) {
            return false;
        }

        return this.elements.has(element);
    }

    remove(element) {
        this.removals.add(element);
    }

    merge(otherSet) {
        this.elements.merge(otherSet.elements);
        this.removals.merge(otherSet.removals);
    }
}

const replicaA = new CRDTSet();
replicaA.add('a');
console.log(replicaA.has('a')); // true

const replicaB = new CRDTSet();
replicaB.merge(replicaA);

replicaA.remove('a');
console.log(replicaA.has('a')); // false

replicaA.merge(replicaB);
console.log(replicaA.has('a')); // false (still false, yey!)
