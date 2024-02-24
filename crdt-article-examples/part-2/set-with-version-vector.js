class VersionVector {
  constructor(pairs) {
    this.pairs = pairs || new Map();
  }

  increment(replicaID) {
    const counter = this.pairs.get(replicaID) || 0;
    return new VersionVector(new Map(this.pairs).set(replicaID, counter + 1));
  }

  merge(otherVersionVector) {
    const mergedPairs = new Map(this.pairs);
    for (let [replicaID, counter] of otherVersionVector.pairs) {
      const existingCounter = mergedPairs.get(replicaID) || 0;
      mergedPairs.set(replicaID, Math.max(existingCounter, counter));
    }
    return new VersionVector(mergedPairs);
  }
}

function createVersionVector(replicaID) {
  return new VersionVector(new Map([[replicaID, 1]]));
}

// The comparison functions could be made more efficient, but I'm splitting them up
// make them easier to understand.

function areVersionVectorsEqual(vv1, vv2) {
  if (vv1.pairs.size !== vv2.pairs.size) {
    return false;
  }
  for (let [replicaID, counter] of vv1.pairs) {
    if (counter !== vv2.pairs.get(replicaID)) {
      return false;
    }
  }
  return true;
}

function isVersionVectorLessOrEqualTo(vv1, vv2) {
  let areAllV1EntriesLessOrEqualToV2 = true;
  for (let [replicaID, counter] of vv1.pairs) {
    const counter2 = vv2.pairs.get(replicaID) || 0;
    if (counter > counter2) {
      areAllV1EntriesLessOrEqualToV2 = false;
      break;
    }
  }
  return areAllV1EntriesLessOrEqualToV2;
}

function compareVersionVectors(vv1, vv2) {
  if (areVersionVectorsEqual(vv1, vv2)) {
    return "equal";
  }

  if (isVersionVectorLessOrEqualTo(vv1, vv2)) {
    return "less";
  }

  if (isVersionVectorLessOrEqualTo(vv2, vv1)) {
    return "greater";
  }

  return "concurrent";
}

class CRDTSet {
  constructor(replicaID) {
    this.replicaID = replicaID;
    this.elements = new Map(); // Map<element, VersionVector>
    this.removals = new Map(); // Map<element, VersionVector>
  }

  $debug() {
    console.log("Replica:", this.replicaID);
    console.log("  Elements:");
    for (let [element, versionVector] of this.elements) {
      console.log("   ->", element, versionVector.pairs);
    }
    console.log("  Removals:");
    for (let [element, versionVector] of this.removals) {
      console.log("   ->", element, versionVector.pairs);
    }
  }

  $getMaxVersionVectorForElement(element) {
    const additionVersionVector = this.elements.get(element);
    const removalVersionVector = this.removals.get(element);
    if (!additionVersionVector && !removalVersionVector) {
      return null;
    } else if (!additionVersionVector) {
      return removalVersionVector;
    } else if (!removalVersionVector) {
      return additionVersionVector;
    } else {
      const result = compareVersionVectors(additionVersionVector, removalVersionVector);
      if (result === "concurrent") {
        throw new Error("version vectors can't be concurrent in the same set");
      }

      return result === "greater"
        ? additionVersionVector
        : removalVersionVector;
    }
  }

  add(element) {
    const maxVersionVector = this.$getMaxVersionVectorForElement(element);
    const newVersionVector = maxVersionVector
      ? maxVersionVector.increment(this.replicaID)
      : createVersionVector(this.replicaID);
    this.elements.set(element, newVersionVector);

    console.log(`Replica '${this.replicaID}' added '${element}' with version vector: `, newVersionVector.pairs);
    this.$debug();
  }

  remove(element) {
    const maxVersionVector = this.$getMaxVersionVectorForElement(element);
    const newVersionVector = maxVersionVector
      ? maxVersionVector.increment(this.replicaID)
      : createVersionVector(this.replicaID);
    this.removals.set(element, newVersionVector);

    console.log(`Replica '${this.replicaID}' removed '${element}' with version vector: `, newVersionVector.pairs);
    this.$debug();
  }

  has(element) {
    const additionVersionVector = this.elements.get(element);
    if (!additionVersionVector) {
      return false;
    }

    const removalVersionVector = this.removals.get(element);
    if (removalVersionVector) {
      const comparison = compareVersionVectors(additionVersionVector, removalVersionVector);
      if (comparison === "concurrent") {
        throw new Error("version vectors can't be concurrent for the same element in the same set");
      }

      if (comparison === "less") {
        return false;
      } else {
        return true;
      }
    }

    return true;
  }

  merge(otherSet) {
    const allElements = new Set([...this.elements.keys(), ...otherSet.elements.keys()]);

    for (const element of allElements) {
      const thisAdditionVersionVector = this.elements.get(element);
      const otherAdditionVersionVector = otherSet.elements.get(element);
      const otherRemovalVersionVector = otherSet.removals.get(element);

      // Element only exists in the other set
      if (!thisAdditionVersionVector) {
        if (otherAdditionVersionVector) {
          this.elements.set(element, otherAdditionVersionVector);
        }
        if (otherRemovalVersionVector) {
          this.removals.set(element, otherRemovalVersionVector);
        }
        continue
      }

      // Element does not exist in the other set, no need to do anything
      if (!otherAdditionVersionVector) {
        continue;
      }

      // Element exists in both sets
      const thisMaxVersionVector = this.$getMaxVersionVectorForElement(element);
      const otherMaxVersionVector = otherSet.$getMaxVersionVectorForElement(element);
      const comparison = compareVersionVectors(thisMaxVersionVector, otherMaxVersionVector);
      if (comparison === "equal") {
        // Elements are equal, no need to do anything
        continue;
      } else if (comparison === "greater") {
        // The other set has an older version of the element, no need to do anything
        continue;
      } else if (comparison === "less") {
        // The other set has a newer version of the element, override the current version
        this.elements.set(element, otherAdditionVersionVector);
        this.removals.set(element, otherRemovalVersionVector);
      } else {
        // Elements are concurrent, so we need to resolve the conflict
        // Concurrent additions take precedence over removals

        const mergedVersionVector = thisMaxVersionVector.merge(otherMaxVersionVector);
        if (this.has(element) && otherSet.has(element)) {
          // Both sets have the element, so we just need to update the version vector
          this.elements.set(element, mergedVersionVector);
        } else if (this.has(element) && !otherSet.has(element)) {
          // This set has the element, but the other set does not
          // Additions take precedence over removals, so we don't remove the element,
          // just update the version vector
          this.elements.set(element, mergedVersionVector);
        } else if (!this.has(element) && otherSet.has(element)) {
          // The other set has the element, but this set does not
          // Additions take precedence over removals, so bring back the element to life
          this.elements.set(element, mergedVersionVector);
        } else if (!this.has(element) && !otherSet.has(element)) {
          // Both sets have removed the element, just update the version vector
          this.removals.set(element, mergedVersionVector);
        } else {
          throw new Error("This should never happen");
        }
      }
    }

    console.log(`Replica '${this.replicaID}' merged with replica '${otherSet.replicaID}'`);
    this.$debug();
  }
}

const replicaA = new CRDTSet("A");
replicaA.add("a");
console.log(replicaA.has("a")); // true

const replicaB = new CRDTSet("B");
replicaB.merge(replicaA);
console.log(replicaB.has("a")); // true
replicaB.remove("a");
console.log(replicaB.has("a")); // false

replicaA.merge(replicaB);
console.log(replicaA.has("a")); // false

// Add after remove
replicaA.add("a");
console.log(replicaA.has("a")); // true

// Let's test concurrent updates
replicaA.add("b");
console.log(replicaA.has("b")); // true
replicaB.merge(replicaA);
console.log(replicaB.has("b")); // true

replicaA.remove("b");
replicaB.remove("b");
console.log(replicaA.has("b")); // false
console.log(replicaB.has("b")); // false

// Replica B adds 'b' back
replicaB.add("b");
console.log(replicaB.has("b")); // true

replicaA.merge(replicaB);
console.log(replicaA.has("b")); // true <- b is back as expected, because the addition in B takes precedence over the removal in A