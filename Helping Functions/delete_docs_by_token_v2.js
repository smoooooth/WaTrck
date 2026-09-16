const { initializeApp, applicationDefault } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');
const fs = require('fs');
const readline = require('readline');

// Uses the Application Default Credentials created by:
// gcloud auth application-default login
initializeApp({
  credential: applicationDefault()
});

const db = getFirestore();
const BATCH_SIZE = 400;



// ============================================================
// ================= WHAT TO DELETE ============================
// ============================================================

// Add the tokens you want to delete here
const TOKENS_TO_DELETE = [
  "MV2ECDH",
  "RC72CYG",
  "77HUZKS",
  "DZ6B87I",
  "KC9BWMV",
  "QH5W79V",
  "50G4O23",
  "BCTOG0N",
  "FE2VII7"
];


// false = ACTUALLY DELETE.
// true  = preview only; nothing gets deleted.
const DRY_RUN = false;

// false = ask "yes" before deleting.
// true  = start deleting immediately when script runs.
const SKIP_CONFIRMATION = false;




function normalizeToken(value) {
    return String(value || '').trim().toUpperCase();
}

function conversationIdFromClick(clickData) {
    const sender =
        clickData.whatsapp_from ||
        clickData.whatsapp_bsuid ||
        clickData.whatsapp_phone ||
        null;
    return sender ? String(sender).replace(/\//g, '_') : null;
}

function timestampMillis(value) {
    if (!value) return 0;
    if (typeof value.toMillis === 'function') return value.toMillis();
    if (typeof value.toDate === 'function') return value.toDate().getTime();
    if (typeof value === 'number') return value * (value < 1e12 ? 1000 : 1);
    const parsed = new Date(value).getTime();
    return Number.isFinite(parsed) ? parsed : 0;
}
async function deleteRefsInBatches(refs, dryRun) {
    if (!refs.length) return 0;
    if (dryRun) return refs.length;
    let deleted = 0;
    for (let i = 0; i < refs.length; i += BATCH_SIZE) {
        const batch = db.batch();
        const chunk = refs.slice(i, i + BATCH_SIZE);
        chunk.forEach(ref => batch.delete(ref));
        await batch.commit();
        deleted += chunk.length;
    }
    return deleted;
}
async function resolveClickDocs(token) {
    const found = new Map();
    const tokenIndexRef = db.doc(`tokenIndex/${token}`);
    const indexSnap = await tokenIndexRef.get();
    if (indexSnap.exists) {
        const indexData = indexSnap.data() || {};
        if (indexData.clickPath) {
            const snap = await db.doc(indexData.clickPath).get();
            if (snap.exists) found.set(snap.ref.path, snap);
        }
        if (indexData.projectId) {
            const snap = await db.doc(`projects/${indexData.projectId}/clicks/${token}`).get();
            if (snap.exists) found.set(snap.ref.path, snap);
        }
    }
    // Catch stale/missing tokenIndex entries and duplicated legacy copies.
    const query = await db
        .collectionGroup('clicks')
        .where('token', '==', token)
        .get();
    query.forEach(doc => found.set(doc.ref.path, doc));
    return {
        tokenIndexRef,
        tokenIndexExists: indexSnap.exists,
        clickDocs: Array.from(found.values())
    };
}

function messageBelongsToToken(doc, clickData, token) {
    const data = doc.data() || {};
    const trackedMessageId = clickData.whatsapp_msg_id ?
        String(clickData.whatsapp_msg_id) :
        '';
    if (trackedMessageId && doc.id === trackedMessageId) return true;
    const text = String(data.text_body || '');
    if (!text) return false;
    const escaped = token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`#${escaped}(?![A-Z0-9])`, 'i').test(text);
}
async function refreshConversationParent(conversationRef, remainingDocs, dryRun) {
    if (!remainingDocs.length || dryRun) return;
    let latest = remainingDocs[0];
    let latestMs = -1;
    for (const doc of remainingDocs) {
        const data = doc.data() || {};
        const ms = Math.max(
            timestampMillis(data.stored_at),
            timestampMillis(data.meta_timestamp)
        );
        if (ms > latestMs) {
            latest = doc;
            latestMs = ms;
        }
    }
    const latestData = latest.data() || {};
    const update = {
        last_message_preview: String(latestData.text_body || '').substring(0, 50)
    };
    if (latestData.stored_at) {
        update.last_active = latestData.stored_at;
    }
    await conversationRef.set(update, {
        merge: true
    });
}
async function cleanupConversationAtPath({
    conversationRef,
    senderIndexRef,
    expectedProjectId,
    clickData,
    token,
    fullChat,
    dryRun,
    label
}) {
    const messagesRef = conversationRef.collection('messages');
    const before = await messagesRef.get();
    if (before.empty) {
        const parent = await conversationRef.get();
        if (parent.exists) {
            console.log(` ${label}: parent exists but has no messages -> delete parent`);
            if (!dryRun) await conversationRef.delete();
        }
        if (senderIndexRef) {
            const senderSnap = await senderIndexRef.get();
            if (
                senderSnap.exists &&
                (!expectedProjectId || senderSnap.data().projectId === expectedProjectId)
            ) {
                console.log(` sender index: delete ${senderIndexRef.path}`);
                if (!dryRun) await senderIndexRef.delete();
            }
        }
        return {
            messagesDeleted: 0,
            conversationDeleted: parent.exists
        };
    }
    let targets;
    if (fullChat) {
        targets = before.docs;
    } else {
        targets = before.docs.filter(doc =>
            messageBelongsToToken(doc, clickData, token)
        );
    }
    if (!targets.length) {
        console.log(` ${label}: no message matched token ${token}; left conversation untouched`);
        return {
            messagesDeleted: 0,
            conversationDeleted: false
        };
    }
    console.log(
        ` ${label}: ${fullChat ? 'delete ALL' : 'delete matched'} message(s): ${targets.length}`
    );
    await deleteRefsInBatches(targets.map(doc => doc.ref), dryRun);
    if (fullChat) {
        console.log(` ${label}: delete conversation parent ${conversationRef.path}`);
        if (!dryRun) await conversationRef.delete();
        if (senderIndexRef) {
            const senderSnap = await senderIndexRef.get();
            if (
                senderSnap.exists &&
                (!expectedProjectId || senderSnap.data().projectId === expectedProjectId)
            ) {
                console.log(` sender index: delete ${senderIndexRef.path}`);
                if (!dryRun) await senderIndexRef.delete();
            }
        }
        return {
            messagesDeleted: targets.length,
            conversationDeleted: true
        };
    }
    // SAFE mode: if other messages remain, preserve the conversation and sender index.
    // If no messages remain, remove the empty parent and sender index as well.
    let remainingDocs;
    if (dryRun) {
        const deletedIds = new Set(targets.map(doc => doc.id));
        remainingDocs = before.docs.filter(doc => !deletedIds.has(doc.id));
    } else {
        const after = await messagesRef.get();
        remainingDocs = after.docs;
    }
    if (!remainingDocs.length) {
        console.log(` ${label}: conversation is empty -> delete parent`);
        if (!dryRun) await conversationRef.delete();
        if (senderIndexRef) {
            const senderSnap = await senderIndexRef.get();
            if (
                senderSnap.exists &&
                (!expectedProjectId || senderSnap.data().projectId === expectedProjectId)
            ) {
                console.log(` sender index: delete ${senderIndexRef.path}`);
                if (!dryRun) await senderIndexRef.delete();
            }
        }
        return {
            messagesDeleted: targets.length,
            conversationDeleted: true
        };
    }
    console.log(
        ` ${label}: ${remainingDocs.length} other message(s) remain -> preserve conversation`
    );
    await refreshConversationParent(conversationRef, remainingDocs, dryRun);
    return {
        messagesDeleted: targets.length,
        conversationDeleted: false
    };
}
async function cleanupWhatsappForClick(clickSnap, token, options) {
    const clickData = clickSnap.data() || {};
    const pathParts = clickSnap.ref.path.split('/');
    const projectId = clickData.projectId || pathParts[1] || null;
    const conversationId = conversationIdFromClick(clickData);
    if (!projectId || !conversationId) {
        console.log(' WhatsApp: no stored project/sender identity -> nothing to clean');
        return;
    }
    const senderIndexRef = db.doc(`whatsappSenderIndex/${conversationId}`);
    // Current project-organized conversation storage.
    await cleanupConversationAtPath({
        conversationRef: db.doc(
            `projects/${projectId}/whatsapp_conversations/${conversationId}`
        ),
        senderIndexRef,
        expectedProjectId: projectId,
        clickData,
        token,
        fullChat: options.fullChat,
        dryRun: options.dryRun,
        label: 'project conversation'
    });
    // Legacy global conversation storage, if old test data still exists.
    await cleanupConversationAtPath({
        conversationRef: db.doc(`whatsapp_conversations/${conversationId}`),
        senderIndexRef: null,
        expectedProjectId: projectId,
        clickData,
        token,
        fullChat: options.fullChat,
        dryRun: options.dryRun,
        label: 'legacy conversation'
    });
}
async function deleteOneToken(token, options) {
    console.log(`\n ${token}`);
    const resolved = await resolveClickDocs(token);
    console.log(
        ` tokenIndex: ${resolved.tokenIndexExists ? 'found' : 'not found'} | click docs: ${resolved.clickDocs.length}`
    );
    if (!resolved.clickDocs.length && !resolved.tokenIndexExists) {
        console.log(' Nothing found.');
        return {
            token,
            clickDocsDeleted: 0,
            tokenIndexDeleted: false
        };
    }
    // WhatsApp cleanup happens BEFORE deleting click docs because click docs hold
    // whatsapp_from / whatsapp_msg_id, which are needed to locate the chat.
// Log the click documents being deleted.
// WhatsApp conversations/messages are intentionally preserved.
            for (const clickSnap of resolved.clickDocs) {
            console.log(`   Click: ${clickSnap.ref.path}`);
            }

console.log('   WhatsApp conversations/messages: PRESERVED');
    const clickRefs = resolved.clickDocs.map(snap => snap.ref);
    if (clickRefs.length) {
        console.log(` Delete click doc(s): ${clickRefs.length}`);
        await deleteRefsInBatches(clickRefs, options.dryRun);
    }
    if (resolved.tokenIndexExists) {
        console.log(` Delete tokenIndex: ${resolved.tokenIndexRef.path}`);
        if (!options.dryRun) await resolved.tokenIndexRef.delete();
    }
    return {
        token,
        clickDocsDeleted: clickRefs.length,
        tokenIndexDeleted: resolved.tokenIndexExists
    };
}




function readTokensFromArgs() {
  const tokens = TOKENS_TO_DELETE
    .map(normalizeToken)
    .filter(Boolean);

  return {
    tokens: [...new Set(tokens)],

        options: {
        dryRun: DRY_RUN,
        yes: SKIP_CONFIRMATION
        }
  };
}





async function askConfirmation(tokens, options) {
    if (options.dryRun || options.yes) return true;
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });
    const mode = options.fullChat ?
        'FULL CHAT (entire sender conversation will be removed)' :
        'SAFE (only token-linked message(s); shared chat preserved)';
    const answer = await new Promise(resolve => {
        rl.question(
            `\nMode: ${mode}\nDelete ${tokens.length} token(s)? Type yes to continue: `,
            value => {
                rl.close();
                resolve(value);
            }
        );
    });
    return String(answer).trim().toLowerCase() === 'yes';
}
async function main() {
    const {
        tokens,
        options
    } = readTokensFromArgs();
    if (!tokens.length) {
        console.log('Usage:');
        console.log(' node delete_docs_by_token_v2.js --tokens TOKEN1,TOKEN2');
        console.log(' node delete_docs_by_token_v2.js --file tokens.txt');
        console.log('');
        console.log('Options:');
        console.log(' --dry-run Show what would be deleted without deleting');
        console.log(' --full-chat Delete the entire WhatsApp conversation for the sender');
        console.log(' --yes Skip confirmation prompt');
        process.exit(0);
    }
    console.log(' WaTrck token cleanup');
    console.log(`Tokens: ${tokens.join(', ')}`);
    console.log(`Dry run: ${options.dryRun ? 'YES' : 'NO'}`);
    console.log(`WhatsApp mode: ${options.fullChat ? 'FULL CHAT' : 'SAFE'}`);
    if (options.fullChat) {
        console.log(
            '⚠ FULL CHAT can remove messages belonging to other tokens if the same WhatsApp sender was reused.'
        );
    }
    const confirmed = await askConfirmation(tokens, options);
    if (!confirmed) {
        console.log('Cancelled.');
        process.exit(0);
    }
    const results = [];
    for (const token of tokens) {
        try {
            results.push(await deleteOneToken(token, options));
        } catch (error) {
            console.error(`  Failed ${token}:`, error.stack || error);
            results.push({
                token,
                error: String(error.message || error)
            });
        }
    }
    console.log('\n Summary');
    results.forEach(result => {
        if (result.error) {
            console.log(` ${result.token}: ERROR - ${result.error}`);
        } else {
            console.log(
                ` ${result.token}: click docs=${result.clickDocsDeleted}, tokenIndex=${result.tokenIndexDeleted ? 'deleted' : 'not found'}`
            );
        }
    });
    if (options.dryRun) {
        console.log('\nDRY RUN ONLY — nothing was deleted.');
    } else {
        console.log('\n Cleanup finished.');
    }
}
if (require.main === module) {
    main()
        .then(() => process.exit(0))
        .catch(error => {
            console.error(' Script failed:', error.stack || error);
            process.exit(1);
        });
}
module.exports = {
    resolveClickDocs,
    cleanupWhatsappForClick,
    deleteOneToken
};