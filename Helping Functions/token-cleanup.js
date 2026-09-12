const admin = require("firebase-admin");

// Initialize Firebase Admin
admin.apps.length || admin.initializeApp();
const db = admin.firestore();

// Tokens to KEEP - DO NOT DELETE THESE
const TOKENS_TO_KEEP = new Set([
  "0Q90TVS",
  "0YN84FZ", 
  "4DVKAA7",
  "KU4KNW6",
  "VCB0T0O",
  "6LXCUWM",
  "VO9CFUB",
  "XYOYYK6"
]);

async function cleanTokenIndex() {
  console.log("🚀 Starting tokenIndex cleanup...");
  console.log(`Tokens to keep (${TOKENS_TO_KEEP.size}):`, Array.from(TOKENS_TO_KEEP));
  console.log("All other tokens will be DELETED.\n");
  
  try {
    // Get all documents in tokenIndex collection
    const snapshot = await db.collection("tokenIndex").get();
    const totalDocs = snapshot.size;
    
    console.log(`Found ${totalDocs} documents in tokenIndex.`);
    
    let deletedCount = 0;
    let keptCount = 0;
    const batch = db.batch();
    let batchSize = 0;
    const MAX_BATCH_SIZE = 500;
    
    // Process each document
    snapshot.forEach((doc) => {
      const token = doc.id;
      
      if (TOKENS_TO_KEEP.has(token)) {
        keptCount++;
      } else {
        // Delete this document
        batch.delete(doc.ref);
        batchSize++;
        deletedCount++;
        
        // Commit batch if we reach max size
        if (batchSize >= MAX_BATCH_SIZE) {
          console.log(`  Committing batch of ${batchSize} deletes...`);
          batch.commit();
          batchSize = 0;
        }
      }
    });
    
    // Commit any remaining deletes
    if (batchSize > 0) {
      console.log(`  Committing final batch of ${batchSize} deletes...`);
      await batch.commit();
    }
    
    console.log("\n✅ Cleanup complete!");
    console.log(`Total documents: ${totalDocs}`);
    console.log(`Deleted: ${deletedCount}`);
    console.log(`Kept: ${keptCount}`);
    
    // Verify the kept tokens still exist
    console.log("\n🔍 Verifying kept tokens still exist...");
    for (const token of TOKENS_TO_KEEP) {
      const doc = await db.collection("tokenIndex").doc(token).get();
      if (doc.exists) {
        console.log(`  ✓ ${token} exists`);
      } else {
        console.log(`  ⚠️  ${token} NOT FOUND (should have been kept)`);
      }
    }
    
  } catch (error) {
    console.error("❌ Error during cleanup:", error);
    process.exit(1);
  }
}

// Run the cleanup
cleanTokenIndex().then(() => {
  console.log("\n✨ Script finished!");
  process.exit(0);
}).catch(error => {
  console.error("❌ Script failed:", error);
  process.exit(1);
});


