const admin = require("firebase-admin");

// Initialize Firebase Admin
admin.apps.length || admin.initializeApp();
const db = admin.firestore();

// Array of tokens to delete - ADD YOUR TOKENS HERE
const TOKENS_TO_DELETE = [
  // Add your tokens here, for example:
  // "ABC123",
  // "DEF456",
  // "GHI789"
];

// Configuration
const BATCH_SIZE = 400; // Firestore batch limit is 500, using 400 for safety
const ALL_PROJECTS = ["AIDA_Muscat", "DaVinci_Tower"]; // Add all your projects

async function deleteDocumentsByTokens() {
  console.log("🚀 Starting deletion of documents by tokens...\n");
  
  if (TOKENS_TO_DELETE.length === 0) {
    console.log("❌ No tokens specified in TOKENS_TO_DELETE array.");
    console.log("   Please add tokens to delete in the script.");
    process.exit(1);
  }
  
  console.log(`Tokens to delete: ${TOKENS_TO_DELETE.length}`);
  console.log(TOKENS_TO_DELETE.join(", "));
  console.log("\nThis will delete documents from:");
  console.log("  1. All project collections (clicks)");
  console.log("  2. tokenIndex collection");
  console.log("\n⚠️  WARNING: This operation is irreversible!\n");
  
  // Ask for confirmation
  const readline = require('readline');
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  const confirm = await new Promise((resolve) => {
    rl.question(`Are you sure you want to delete ${TOKENS_TO_DELETE.length} documents? (yes/no): `, (answer) => {
      rl.close();
      resolve(answer.toLowerCase() === 'yes' || answer.toLowerCase() === 'y');
    });
  });
  
  if (!confirm) {
    console.log("Operation cancelled.");
    process.exit(0);
  }
  
  try {
    let totalDeletions = 0;
    let batchCount = 0;
    let tokensFound = 0;
    let tokensNotFound = 0;
    
    // Process tokens in batches
    for (let i = 0; i < TOKENS_TO_DELETE.length; i += BATCH_SIZE / 2) {
      batchCount++;
      const batch = db.batch();
      let batchOperations = 0;
      
      const chunkEnd = Math.min(i + (BATCH_SIZE / 2), TOKENS_TO_DELETE.length);
      
      for (let j = i; j < chunkEnd; j++) {
        const token = TOKENS_TO_DELETE[j];
        
        // === 1. Delete from tokenIndex collection ===
        const tokenIndexRef = db.doc(`tokenIndex/${token}`);
        batch.delete(tokenIndexRef);
        batchOperations++;
        
        // === 2. Delete from all project collections ===
        // We need to find which project the token belongs to
        // First, check tokenIndex to get the project (if it exists)
        // If not, try all projects
        
        // We'll handle this by trying to delete from all projects
        // This is safe because delete() on a non-existent document does nothing
        for (const projectId of ALL_PROJECTS) {
          const clickDocRef = db.doc(`projects/${projectId}/clicks/${token}`);
          batch.delete(clickDocRef);
          batchOperations++;
        }
        
        totalDeletions += (1 + ALL_PROJECTS.length); // tokenIndex + all projects
      }
      
      // Commit the batch
      console.log(`  Committing batch ${batchCount} (${batchOperations} operations, ${chunkEnd}/${TOKENS_TO_DELETE.length} tokens)...`);
      await batch.commit();
      
      // Small delay to avoid hitting rate limits
      if (batchCount % 5 === 0) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    console.log("\n✅ Batch deletions complete!");
    console.log(`Total batch operations: ${totalDeletions}`);
    console.log(`Batches committed: ${batchCount}`);
    
    // === 3. Verify deletions ===
    console.log("\n🔍 Verifying deletions...");
    
    // Check tokenIndex deletions
    const tokenIndexResults = [];
    for (const token of TOKENS_TO_DELETE.slice(0, 10)) { // Check first 10
      const doc = await db.doc(`tokenIndex/${token}`).get();
      if (doc.exists) {
        tokenIndexResults.push({ token, exists: true });
        tokensFound++;
      } else {
        tokenIndexResults.push({ token, exists: false });
        tokensNotFound++;
      }
    }
    
    console.log("\nTokenIndex verification (first 10 tokens):");
    tokenIndexResults.forEach(result => {
      console.log(`  ${result.token}: ${result.exists ? '❌ STILL EXISTS' : '✅ Deleted'}`);
    });
    
    // Check project collections for a sample token
    if (TOKENS_TO_DELETE.length > 0) {
      const sampleToken = TOKENS_TO_DELETE[0];
      console.log(`\n🔍 Checking project collections for token: ${sampleToken}`);
      
      for (const projectId of ALL_PROJECTS.slice(0, 3)) { // Check first 3 projects
        const doc = await db.doc(`projects/${projectId}/clicks/${sampleToken}`).get();
        console.log(`  ${projectId}: ${doc.exists ? '❌ Document exists' : '✅ Document deleted or never existed'}`);
      }
    }
    
    // Summary
    console.log("\n📊 Summary:");
    console.log(`   Tokens processed: ${TOKENS_TO_DELETE.length}`);
    console.log(`   TokenIndex documents found: ${tokensFound}`);
    console.log(`   TokenIndex documents not found: ${tokensNotFound}`);
    console.log(`   Attempted deletions from ${ALL_PROJECTS.length} projects`);
    
    if (tokensFound > 0) {
      console.log(`\n⚠️  Warning: Some tokenIndex documents still exist after deletion attempt.`);
      console.log(`   This could mean:`);
      console.log(`   1. The documents didn't exist in the first place`);
      console.log(`   2. There was an error during deletion`);
      console.log(`   3. The tokens have different IDs than expected`);
    } else {
      console.log("\n✅ All tokenIndex documents appear to be deleted.");
    }
    
  } catch (error) {
    console.error("❌ Error during deletion:", error);
    console.error("Stack trace:", error.stack);
    process.exit(1);
  }
}

// Alternative: More efficient version that checks tokenIndex first
async function deleteDocumentsByTokensEfficient() {
  console.log("🚀 Starting efficient deletion of documents by tokens...\n");
  
  if (TOKENS_TO_DELETE.length === 0) {
    console.log("❌ No tokens specified in TOKENS_TO_DELETE array.");
    console.log("   Please add tokens to delete in the script.");
    process.exit(1);
  }
  
  console.log(`Tokens to delete: ${TOKENS_TO_DELETE.length}`);
  console.log(TOKENS_TO_DELETE.join(", "));
  console.log("\nThis method first checks tokenIndex to find the correct project,");
  console.log("then deletes only from that project (more efficient).\n");
  
  // Ask for confirmation
  const readline = require('readline');
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  const confirm = await new Promise((resolve) => {
    rl.question(`Are you sure you want to delete ${TOKENS_TO_DELETE.length} documents? (yes/no): `, (answer) => {
      rl.close();
      resolve(answer.toLowerCase() === 'yes' || answer.toLowerCase() === 'y');
    });
  });
  
  if (!confirm) {
    console.log("Operation cancelled.");
    process.exit(0);
  }
  
  try {
    let totalDeletions = 0;
    let batchCount = 0;
    let tokensWithTokenIndex = 0;
    let tokensWithoutTokenIndex = 0;
    const deletionReport = [];
    
    // First, gather all tokenIndex documents to find project info
    console.log("🔍 Checking tokenIndex for project information...");
    
    const tokenIndexDocs = [];
    for (const token of TOKENS_TO_DELETE) {
      const tokenIndexRef = db.doc(`tokenIndex/${token}`);
      const doc = await tokenIndexRef.get();
      
      if (doc.exists) {
        const data = doc.data();
        tokenIndexDocs.push({
          token,
          tokenIndexRef,
          projectId: data.projectId,
          clickPath: data.clickPath
        });
        tokensWithTokenIndex++;
      } else {
        tokenIndexDocs.push({
          token,
          tokenIndexRef,
          projectId: null,
          clickPath: null
        });
        tokensWithoutTokenIndex++;
      }
    }
    
    console.log(`Found tokenIndex entries for ${tokensWithTokenIndex} tokens`);
    console.log(`No tokenIndex entries for ${tokensWithoutTokenIndex} tokens`);
    
    // Process in batches
    for (let i = 0; i < tokenIndexDocs.length; i += BATCH_SIZE / 2) {
      batchCount++;
      const batch = db.batch();
      let batchOperations = 0;
      
      const chunkEnd = Math.min(i + (BATCH_SIZE / 2), tokenIndexDocs.length);
      
      for (let j = i; j < chunkEnd; j++) {
        const { token, tokenIndexRef, projectId, clickPath } = tokenIndexDocs[j];
        
        // Always delete from tokenIndex
        batch.delete(tokenIndexRef);
        batchOperations++;
        
        // Delete from click collection if we know the project
        if (projectId && clickPath) {
          // Use the clickPath from tokenIndex
          const clickDocRef = db.doc(clickPath);
          batch.delete(clickDocRef);
          batchOperations++;
          
          deletionReport.push({
            token,
            status: 'deleted',
            projectId,
            clickPath
          });
        } else {
          // Try all projects as fallback
          for (const project of ALL_PROJECTS) {
            const clickDocRef = db.doc(`projects/${project}/clicks/${token}`);
            batch.delete(clickDocRef);
            batchOperations++;
          }
          
          deletionReport.push({
            token,
            status: 'deleted_fallback',
            projectId: 'unknown',
            note: 'Tried all projects'
          });
        }
        
        totalDeletions += batchOperations - (j === i ? 0 : batchOperations); // Count per token
      }
      
      // Commit the batch
      console.log(`  Committing batch ${batchCount} (${batchOperations} operations, ${chunkEnd}/${TOKENS_TO_DELETE.length} tokens)...`);
      await batch.commit();
      
      // Small delay to avoid hitting rate limits
      if (batchCount % 5 === 0) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    console.log("\n✅ Deletion complete!");
    console.log(`Total operations: ${totalDeletions}`);
    console.log(`Batches committed: ${batchCount}`);
    
    // Summary report
    console.log("\n📊 Deletion Report:");
    
    const deletedWithPath = deletionReport.filter(r => r.status === 'deleted').length;
    const deletedWithFallback = deletionReport.filter(r => r.status === 'deleted_fallback').length;
    
    console.log(`   Tokens with tokenIndex: ${tokensWithTokenIndex}`);
    console.log(`   Tokens without tokenIndex: ${tokensWithoutTokenIndex}`);
    console.log(`   Deleted with known path: ${deletedWithPath}`);
    console.log(`   Deleted with fallback: ${deletedWithFallback}`);
    
    // Show sample of deletions
    console.log("\n🔍 Sample deletions (first 5):");
    deletionReport.slice(0, 5).forEach((report, index) => {
      console.log(`\n  ${index + 1}. Token: ${report.token}`);
      console.log(`     Status: ${report.status}`);
      if (report.projectId !== 'unknown') {
        console.log(`     Project: ${report.projectId}`);
        console.log(`     Path: ${report.clickPath}`);
      } else {
        console.log(`     Note: ${report.note}`);
      }
    });
    
  } catch (error) {
    console.error("❌ Error during deletion:", error);
    console.error("Stack trace:", error.stack);
    process.exit(1);
  }
}

// Function to delete by reading tokens from command line
async function deleteFromCommandLine() {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.log("Usage:");
    console.log("  node delete_docs_by_token.js --tokens token1,token2,token3");
    console.log("  node delete_docs_by_token.js --file tokens.txt");
    console.log("\nOr edit the TOKENS_TO_DELETE array in the script.");
    process.exit(0);
  }
  
  let tokens = [];
  
  if (args[0] === '--tokens' && args[1]) {
    tokens = args[1].split(',').map(t => t.trim()).filter(t => t);
  } else if (args[0] === '--file' && args[1]) {
    const fs = require('fs');
    const fileContent = fs.readFileSync(args[1], 'utf8');
    tokens = fileContent.split('\n')
      .map(line => line.trim())
      .filter(line => line && !line.startsWith('#'));
  } else {
    console.log("Invalid arguments. Use --tokens or --file flag.");
    process.exit(1);
  }
  
  console.log(`Found ${tokens.length} tokens to delete.`);
  
  // Use the efficient method
  TOKENS_TO_DELETE.length = 0; // Clear array
  TOKENS_TO_DELETE.push(...tokens); // Add tokens from command line
  
  await deleteDocumentsByTokensEfficient();
}

// Main execution
if (require.main === module) {
  const args = process.argv.slice(2);
  
  if (args.length > 0 && (args[0] === '--tokens' || args[0] === '--file')) {
    deleteFromCommandLine().then(() => {
      console.log("\n✨ Script finished!");
      process.exit(0);
    }).catch(error => {
      console.error("❌ Script failed:", error);
      process.exit(1);
    });
  } else if (TOKENS_TO_DELETE.length > 0) {
    // Use the efficient method by default
    deleteDocumentsByTokensEfficient().then(() => {
      console.log("\n✨ Script finished!");
      process.exit(0);
    }).catch(error => {
      console.error("❌ Script failed:", error);
      process.exit(1);
    });
  } else {
    console.log("No tokens specified. You have two options:");
    console.log("\n1. Edit the TOKENS_TO_DELETE array in the script");
    console.log("2. Run with command line arguments:");
    console.log("   node delete_docs_by_token.js --tokens token1,token2,token3");
    console.log("   node delete_docs_by_token.js --file tokens.txt");
    process.exit(0);
  }
}

module.exports = { deleteDocumentsByTokens, deleteDocumentsByTokensEfficient };


