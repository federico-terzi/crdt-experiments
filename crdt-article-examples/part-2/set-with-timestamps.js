function waitFor(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class CRDTSet {
  constructor() {
    this.elements = new Map();
    this.removals = new Map();
  }

  add(element) {
    this.elements.set(element, new Date().getTime());
  }

  has(element) {
    const additionTime = this.elements.get(element);
    if (additionTime === undefined) {
      return false;
    }

    const removalTime = this.removals.get(element);
    if (removalTime) {
      return removalTime <= additionTime;
    }

    return true;
  }

  remove(element) {
    this.removals.set(element, new Date().getTime());
  }

  merge(otherSet) {
    for (let [element, additionTime] of otherSet.elements) {
      const existingAdditionTime = this.elements.get(element);
      if (
        existingAdditionTime === undefined ||
        existingAdditionTime < additionTime
      ) {
        this.elements.set(element, additionTime);
      }
    }

    for (let [element, removalTime] of otherSet.removals) {
      const existingRemovalTime = this.removals.get(element);
      if (
        existingRemovalTime === undefined ||
        existingRemovalTime < removalTime
      ) {
        this.removals.set(element, removalTime);
      }
    }
  }
}

// Making the function async so that we can use await
async function main() {
  const replicaA = new CRDTSet();
  replicaA.add("a");
  console.log(replicaA.has("a")); // true

  // Wait for some time between operation.
  // This way, the new Date.getTime() call can't return the same value
  await waitFor(10);

  const replicaB = new CRDTSet();
  replicaB.merge(replicaA);
  replicaB.remove("a");
  console.log(replicaB.has("a")); // false

  await waitFor(10);

  replicaA.merge(replicaB);
  console.log(replicaA.has("a")); // false

  await waitFor(10);

  replicaA.add("a");
  console.log(replicaA.has("a")); // true! Yay, 'a' is back!
}

main();
