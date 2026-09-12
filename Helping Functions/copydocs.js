const admin = require("firebase-admin");

// Initialize Firebase Admin
admin.apps.length || admin.initializeApp();
const db = admin.firestore();

// Source and destination collections
const SOURCE_COLLECTION = "DVT_Pagani";
const DEST_COLLECTION = "DaVinci_Tower";
const BATCH_SIZE = 400; // Reduced to account for 2 operations per doc

async function copyDocuments() {
  console.log(`🚀 Starting document copy from ${SOURCE_COLLECTION} to ${DEST_COLLECTION}...\n`);
  
  try {
    // Get all documents from source collection
    const sourcePath = `projects/${SOURCE_COLLECTION}/clicks`;
    const sourceSnapshot = await db.collection(sourcePath).get();
    const totalDocs = sourceSnapshot.size;
    
    console.log(`Found ${totalDocs} documents in ${SOURCE_COLLECTION}.`);
    
    if (totalDocs === 0) {
      console.log("No documents to copy.");
      return;
    }
    
    let copiedCount = 0;
    let tokenIndexCount = 0;
    let batchCount = 0;
    
    // Process in batches
    for (let i = 0; i < totalDocs; i += BATCH_SIZE / 2) {
      batchCount++;
      const batch = db.batch();
      let batchOperations = 0;
      
      // Process a chunk of documents
      const chunkEnd = Math.min(i + (BATCH_SIZE / 2), totalDocs);
      
      for (let j = i; j < chunkEnd; j++) {
        const sourceDoc = sourceSnapshot.docs[j];
        const sourceData = sourceDoc.data();
        const docId = sourceDoc.id;
        const token = sourceData.token || docId;
        
        // === 1. Prepare destination document data ===
        const destinationData = { ...sourceData };
        
        // Update projectId to new collection name
        if (destinationData.projectId === SOURCE_COLLECTION) {
          destinationData.projectId = DEST_COLLECTION;
        }
        
        // Add metadata for tracking
        destinationData._copiedFrom = SOURCE_COLLECTION;
        destinationData._copiedAt = admin.firestore.FieldValue.serverTimestamp();
        destinationData._originalId = docId;
        
        // Set the document in the destination collection
        const destRef = db.doc(`projects/${DEST_COLLECTION}/clicks/${docId}`);
        batch.set(destRef, destinationData);
        batchOperations++;
        
        // === 2. Create/update tokenIndex document ===
        const tokenIndexRef = db.doc(`tokenIndex/${token}`);
        
        // Prepare tokenIndex data
        const tokenIndexData = {
          token: token,
          projectId: DEST_COLLECTION,
          clickPath: `projects/${DEST_COLLECTION}/clicks/${docId}`,
          created_at: admin.firestore.FieldValue.serverTimestamp(),
          // Include gclid and google_campaign_id from source data for debugging
          gclid: sourceData.gclid !== undefined ? sourceData.gclid : null,
          google_campaign_id: sourceData.google_campaign_id !== undefined ? sourceData.google_campaign_id : null,
          // Additional debug info
          _createdDuringCopy: true,
          _sourceProject: SOURCE_COLLECTION,
          _sourceToken: token
        };
        
        batch.set(tokenIndexRef, tokenIndexData);
        batchOperations++;
        tokenIndexCount++;
        
        copiedCount++;
      }
      
      // Commit the batch
      console.log(`  Committing batch ${batchCount} (${batchOperations} operations, ${copiedCount}/${totalDocs} docs)...`);
      await batch.commit();
      
      // Small delay to avoid hitting rate limits
      if (batchCount % 5 === 0) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    console.log("\n✅ Copy complete!");
    console.log(`Total documents copied: ${copiedCount}/${totalDocs}`);
    console.log(`TokenIndex documents created/updated: ${tokenIndexCount}`);
    console.log(`Batches committed: ${batchCount}`);
    
    // === 3. Verification ===
    console.log("\n🔍 Running verification...");
    
    // Verify destination collection count
    const destSnapshot = await db.collection(`projects/${DEST_COLLECTION}/clicks`).get();
    console.log(`Documents in destination: ${destSnapshot.size}`);
    
    if (destSnapshot.size === totalDocs) {
      console.log("✅ Document count matches!");
    } else {
      console.log(`⚠️  Warning: Source had ${totalDocs} docs, destination has ${destSnapshot.size} docs`);
    }
    
    // Verify tokenIndex entries
    const tokenIndexSnapshot = await db.collection("tokenIndex")
      .where("projectId", "==", DEST_COLLECTION)
      .get();
    
    console.log(`TokenIndex entries for ${DEST_COLLECTION}: ${tokenIndexSnapshot.size}`);
    
    // Sample verification
    console.log("\n📋 Sample verification (first 3 documents):");
    const sampleDocs = sourceSnapshot.docs.slice(0, 3);
    
    for (const sourceDoc of sampleDocs) {
      const sourceId = sourceDoc.id;
      const sourceData = sourceDoc.data();
      const token = sourceData.token || sourceId;
      
      console.log(`\n  Document: ${sourceId} (Token: ${token})`);
      
      // Check destination document
      const destDoc = await db.doc(`projects/${DEST_COLLECTION}/clicks/${sourceId}`).get();
      if (destDoc.exists) {
        const destData = destDoc.data();
        const sourceKeys = Object.keys(sourceData).sort();
        const destKeys = Object.keys(destData).sort();
        
        console.log(`    ✅ Destination document exists`);
        console.log(`        Original fields: ${sourceKeys.length}`);
        console.log(`        Copied fields: ${destKeys.length}`);
        console.log(`        ProjectId: ${destData.projectId}`);
        
        // Check for any missing fields
        const missingInDest = sourceKeys.filter(key => !destKeys.includes(key) && key !== 'projectId');
        if (missingInDest.length > 0) {
          console.log(`        ⚠️  Missing in copy: ${missingInDest.join(", ")}`);
        }
      } else {
        console.log(`    ❌ Destination document not found!`);
      }
      
      // Check tokenIndex
      const tokenIndexDoc = await db.doc(`tokenIndex/${token}`).get();
      if (tokenIndexDoc.exists) {
        const tokenData = tokenIndexDoc.data();
        console.log(`    ✅ TokenIndex document exists`);
        console.log(`        clickPath: ${tokenData.clickPath}`);
        console.log(`        projectId: ${tokenData.projectId}`);
        console.log(`        gclid: ${tokenData.gclid !== undefined ? tokenData.gclid : 'Not set'}`);
        console.log(`        google_campaign_id: ${tokenData.google_campaign_id !== undefined ? tokenData.google_campaign_id : 'Not set'}`);
      } else {
        console.log(`    ❌ TokenIndex document not found!`);
      }
    }
    
    // Summary
    console.log("\n📊 Summary:");
    console.log(`   Source collection: ${SOURCE_COLLECTION}`);
    console.log(`   Destination collection: ${DEST_COLLECTION}`);
    console.log(`   Documents copied: ${copiedCount}`);
    console.log(`   TokenIndex entries created: ${tokenIndexCount}`);
    console.log(`   All data copied WITHOUT test markers`);
    
  } catch (error) {
    console.error("❌ Error during copy:", error);
    console.error("Stack trace:", error.stack);
    process.exit(1);
  }
}

// Run the copy
copyDocuments().then(() => {
  console.log("\n✨ Script finished!");
  process.exit(0);
}).catch(error => {
  console.error("❌ Script failed:", error);
  process.exit(1);
});


