const admin = require("firebase-admin");
const crypto = require("crypto");

// Initialize Firebase Admin
admin.apps.length || admin.initializeApp();
const db = admin.firestore();

// Configuration - update these if needed
const KNOWN_PROJECTS = ["AIDA_Muscat", "DaVinci_Tower"];
const DEBUG_MODE = true;

async function countEligibleConversions() {
  console.log("🔍 Starting Firestore eligible conversions count...\n");
  
  let totalEligible = 0;
  let totalAll = 0;
  const results = {
    perProject: {},
    collectionGroup: { total: 0, byProject: {} }
  };
  
  // Method 1: Count per project collection
  console.log("📊 METHOD 1: Per-Project Collection Count");
  console.log("=".repeat(50));
  
  for (const projectId of KNOWN_PROJECTS) {
    try {
      // Count ALL documents in the project
      const allSnapshot = await db.collection(`projects/${projectId}/clicks`).get();
      const allCount = allSnapshot.size;
      
      // Count ELIGIBLE documents in the project
      const eligibleQuery = db.collection(`projects/${projectId}/clicks`)
        .where("used", "==", true)
        .where("google_sheet_exported", "==", false);
      
      const eligibleSnapshot = await eligibleQuery.get();
      const eligibleCount = eligibleSnapshot.size;
      
      // Store results
      results.perProject[projectId] = {
        all: allCount,
        eligible: eligibleCount,
        eligiblePercentage: allCount > 0 ? ((eligibleCount / allCount) * 100).toFixed(1) : 0
      };
      
      totalEligible += eligibleCount;
      totalAll += allCount;
      
      console.log(`   ${projectId}:`);
      console.log(`     Total docs: ${allCount}`);
      console.log(`     Eligible (used=true, exported=false): ${eligibleCount}`);
      console.log(`     Eligible %: ${allCount > 0 ? ((eligibleCount / allCount) * 100).toFixed(1) : 0}%`);
      
      if (DEBUG_MODE && eligibleCount > 0) {
        // Sample some eligible documents
        console.log(`     Sample eligible docs (first 3):`);
        eligibleSnapshot.docs.slice(0, 3).forEach(doc => {
          const data = doc.data();
          console.log(`       - ${doc.id}: used=${data.used}, exported=${data.google_sheet_exported}, used_at=${data.used_at?.toDate?.() || data.used_at}`);
        });
      }
      
      console.log();
      
    } catch (error) {
      console.error(`   ❌ Error counting for ${projectId}:`, error.message);
      results.perProject[projectId] = { error: error.message };
    }
  }
  
  // Method 2: CollectionGroup query (catch-all)
  console.log("\n📊 METHOD 2: CollectionGroup Query (All Clicks)");
  console.log("=".repeat(50));
  
  try {
    const collectionGroupQuery = db.collectionGroup("clicks")
      .where("used", "==", true)
      .where("google_sheet_exported", "==", false);
    
    const collectionGroupSnapshot = await collectionGroupQuery.get();
    results.collectionGroup.total = collectionGroupSnapshot.size;
    
    // Group by projectId
    const projectCounts = {};
    collectionGroupSnapshot.forEach(doc => {
      const data = doc.data();
      const projectId = data.projectId || "unknown";
      
      if (!projectCounts[projectId]) {
        projectCounts[projectId] = 0;
      }
      projectCounts[projectId]++;
    });
    
    results.collectionGroup.byProject = projectCounts;
    
    console.log(`   CollectionGroup found ${collectionGroupSnapshot.size} total eligible documents`);
    console.log(`   Breakdown by projectId:`);
    
    Object.entries(projectCounts).sort((a, b) => b[1] - a[1]).forEach(([project, count]) => {
      console.log(`     ${project}: ${count} docs`);
    });
    
    if (DEBUG_MODE && collectionGroupSnapshot.size > 0) {
      // Look for documents in unexpected collections
      const unexpectedPaths = new Set();
      collectionGroupSnapshot.docs.slice(0, 5).forEach(doc => {
        const path = doc.ref.path;
        const expectedPrefix = `projects/${doc.data().projectId || 'unknown'}/clicks`;
        if (!path.startsWith(expectedPrefix)) {
          unexpectedPaths.add(path);
        }
      });
      
      if (unexpectedPaths.size > 0) {
        console.log(`   ⚠️  Documents in unexpected collections:`);
        Array.from(unexpectedPaths).forEach(path => {
          console.log(`     - ${path}`);
        });
      }
    }
    
  } catch (error) {
    console.error(`   ❌ Error in collectionGroup query:`, error.message);
  }
  
  // Method 3: Check consistency between methods
  console.log("\n📊 METHOD 3: Consistency Check");
  console.log("=".repeat(50));
  
  const perProjectSum = Object.values(results.perProject)
    .filter(r => !r.error)
    .reduce((sum, r) => sum + (r.eligible || 0), 0);
  
  const collectionGroupTotal = results.collectionGroup.total || 0;
  
  console.log(`   Per-project sum: ${perProjectSum} eligible docs`);
  console.log(`   CollectionGroup total: ${collectionGroupTotal} eligible docs`);
  console.log(`   Difference: ${Math.abs(perProjectSum - collectionGroupTotal)} docs`);
  
  if (Math.abs(perProjectSum - collectionGroupTotal) > 0) {
    console.log(`   ⚠️  MISMATCH DETECTED! Possible reasons:`);
    console.log(`      - Documents in unexpected collection paths`);
    console.log(`      - Documents with missing/incorrect projectId field`);
    console.log(`      - Documents in test/project collections not in KNOWN_PROJECTS list`);
    
    // Try to identify the discrepancy
    const knownProjectIds = new Set(KNOWN_PROJECTS);
    const unknownProjects = Object.keys(results.collectionGroup.byProject || {})
      .filter(project => !knownProjectIds.has(project) && project !== "unknown");
    
    if (unknownProjects.length > 0) {
      console.log(`   📍 Unknown projects found in collectionGroup:`);
      unknownProjects.forEach(project => {
        console.log(`      ${project}: ${results.collectionGroup.byProject[project]} docs`);
      });
    }
  } else {
    console.log(`   ✅ Counts match!`);
  }
  
  // Method 4: Detailed query analysis
  console.log("\n📊 METHOD 4: Query Performance Analysis");
  console.log("=".repeat(50));
  
  // Test the actual query pattern used by the backend
  console.log("   Testing backend query pattern for each project:");
  
  for (const projectId of KNOWN_PROJECTS.slice(0, 2)) { // Limit to 2 for speed
    try {
      const startTime = Date.now();
      
      // Test without orderBy (old buggy behavior)
      const queryWithoutOrder = db.collection(`projects/${projectId}/clicks`)
        .where("used", "==", true)
        .where("google_sheet_exported", "==", false)
        .limit(100);
      
      const resultWithoutOrder = await queryWithoutOrder.get();
      const timeWithoutOrder = Date.now() - startTime;
      
      // Test with orderBy (fixed behavior)
      const startTime2 = Date.now();
      const queryWithOrder = db.collection(`projects/${projectId}/clicks`)
        .where("used", "==", true)
        .where("google_sheet_exported", "==", false)
        .orderBy("used_at", "asc")
        .limit(100);
      
      let resultWithOrder;
      let timeWithOrder = 0;
      let orderByError = null;
      
      try {
        resultWithOrder = await queryWithOrder.get();
        timeWithOrder = Date.now() - startTime2;
      } catch (error) {
        orderByError = error.message;
        timeWithOrder = Date.now() - startTime2;
      }
      
      console.log(`   ${projectId}:`);
      console.log(`     Without orderBy: ${resultWithoutOrder.size} docs in ${timeWithoutOrder}ms`);
      
      if (orderByError) {
        console.log(`     With orderBy: ERROR - ${orderByError}`);
      } else {
        console.log(`     With orderBy: ${resultWithOrder?.size || 0} docs in ${timeWithOrder}ms`);
      }
      
      // Check if results differ
      if (!orderByError && resultWithoutOrder.size !== resultWithOrder.size) {
        console.log(`     ⚠️  Different counts! Might indicate zigzag pattern issue`);
      }
      
    } catch (error) {
      console.log(`   ${projectId}: Error - ${error.message}`);
    }
  }
  
  // Summary
  console.log("\n" + "✨".repeat(25));
  console.log("✨ SUMMARY OF ELIGIBLE CONVERSIONS ✨");
  console.log("✨".repeat(25));
  
  console.log("\nPER-PROJECT BREAKDOWN:");
  Object.entries(results.perProject).forEach(([project, data]) => {
    if (data.error) {
      console.log(`  ${project}: ERROR - ${data.error}`);
    } else {
      console.log(`  ${project}: ${data.eligible} eligible / ${data.all} total (${data.eligiblePercentage}%)`);
    }
  });
  
  console.log(`\nTOTAL ELIGIBLE (per-project sum): ${perProjectSum}`);
  console.log(`TOTAL ELIGIBLE (collectionGroup): ${collectionGroupTotal}`);
  
  if (totalAll > 0) {
    const overallPercentage = ((totalEligible / totalAll) * 100).toFixed(1);
    console.log(`OVERALL: ${totalEligible} eligible / ${totalAll} total (${overallPercentage}%)`);
  }
  
  // Recommendations based on findings
  console.log("\n🔧 RECOMMENDATIONS:");
  
  if (Math.abs(perProjectSum - collectionGroupTotal) > 0) {
    console.log("  1. Investigate the discrepancy between collectionGroup and per-project counts");
    console.log("  2. Check for documents with incorrect projectId fields or collection paths");
  }
  
  const hasEligibleDocs = totalEligible > 0;
  if (hasEligibleDocs) {
    console.log("  3. There are eligible conversions waiting to be exported");
  } else {
    console.log("  3. No eligible conversions found - system is caught up!");
  }
  
  // Check if all known projects are indexed properly
  console.log("\n🔍 INDEX STATUS (inferred from query errors):");
  for (const projectId of KNOWN_PROJECTS.slice(0, 2)) {
    try {
      await db.collection(`projects/${projectId}/clicks`)
        .where("used", "==", true)
        .where("google_sheet_exported", "==", false)
        .orderBy("used_at", "asc")
        .limit(1)
        .get();
      console.log(`  ${projectId}: ✅ orderBy query works`);
    } catch (error) {
      if (error.message.includes("requires an index")) {
        console.log(`  ${projectId}: ❌ Missing index for orderBy query`);
        console.log(`     Create composite index on: used, google_sheet_exported, used_at`);
      } else {
        console.log(`  ${projectId}: ⚠️  Query error: ${error.message}`);
      }
    }
  }
  
  return results;
}

// Check for command line arguments
const args = process.argv.slice(2);
const specificProject = args.find(arg => arg.startsWith("--project="));
const projectId = specificProject ? specificProject.split("=")[1] : null;

async function runDebug() {
  try {
    if (projectId) {
      console.log(`🔍 Debugging specific project: ${projectId}`);
      
      // Just check this one project
      const query = db.collection(`projects/${projectId}/clicks`)
        .where("used", "==", true)
        .where("google_sheet_exported", "==", false);
      
      const snapshot = await query.get();
      console.log(`\nProject ${projectId} has ${snapshot.size} eligible conversions`);
      
      if (snapshot.size > 0) {
        console.log("\nSample documents:");
        snapshot.docs.slice(0, 5).forEach((doc, index) => {
          const data = doc.data();
          console.log(`\n[${index + 1}] ${doc.id}:`);
          console.log(`   used: ${data.used}`);
          console.log(`   google_sheet_exported: ${data.google_sheet_exported}`);
          console.log(`   used_at: ${data.used_at?.toDate?.() || data.used_at}`);
          console.log(`   created: ${data.ts || data.created_at}`);
          console.log(`   gclid: ${data.gclid ? 'Yes' : 'No'}`);
          console.log(`   conversion_name: ${data.conversion_name}`);
        });
      }
      
    } else {
      await countEligibleConversions();
    }
    
    process.exit(0);
  } catch (error) {
    console.error("❌ Script failed:", error);
    process.exit(1);
  }
}

// Add this helper to check single document
async function checkDocument(token) {
  try {
    console.log(`🔍 Checking document with token: ${token}`);
    
    // Try tokenIndex first
    const tokenIndexDoc = await db.doc(`tokenIndex/${token}`).get();
    if (tokenIndexDoc.exists) {
      const tokenData = tokenIndexDoc.data();
      console.log("📁 TokenIndex entry:");
      console.log(`   projectId: ${tokenData.projectId}`);
      console.log(`   clickPath: ${tokenData.clickPath}`);
      
      // Try to get the click document
      if (tokenData.clickPath) {
        const clickDoc = await db.doc(tokenData.clickPath).get();
        if (clickDoc.exists) {
          const clickData = clickDoc.data();
          console.log("\n📄 Click document:");
          console.log(`   used: ${clickData.used}`);
          console.log(`   google_sheet_exported: ${clickData.google_sheet_exported}`);
          console.log(`   used_at: ${clickData.used_at?.toDate?.() || clickData.used_at}`);
          console.log(`   gclid: ${clickData.gclid || '(empty)'}`);
          console.log(`   Eligible for export?: ${clickData.used === true && clickData.google_sheet_exported !== true ? 'YES' : 'NO'}`);
        } else {
          console.log(`❌ Click document not found at path: ${tokenData.clickPath}`);
        }
      }
    } else {
      console.log("❌ Token not found in tokenIndex");
      
      // Try collectionGroup search
      const query = db.collectionGroup("clicks").where("token", "==", token).limit(1);
      const snapshot = await query.get();
      if (!snapshot.empty) {
        const doc = snapshot.docs[0];
        console.log(`\n📄 Found in collectionGroup at: ${doc.ref.path}`);
        const data = doc.data();
        console.log(`   projectId field: ${data.projectId}`);
        console.log(`   used: ${data.used}`);
        console.log(`   google_sheet_exported: ${data.google_sheet_exported}`);
      } else {
        console.log("❌ Document not found anywhere");
      }
    }
  } catch (error) {
    console.error("Error checking document:", error);
  }
}

// Run the script
if (require.main === module) {
  // Check if we're looking for a specific token
  const tokenArg = args.find(arg => arg.startsWith("--token="));
  if (tokenArg) {
    const token = tokenArg.split("=")[1];
    checkDocument(token).then(() => process.exit(0));
  } else {
    runDebug();
  }
}

module.exports = { countEligibleConversions, checkDocument };

