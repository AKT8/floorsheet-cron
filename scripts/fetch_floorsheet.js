import axios from "axios";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  { auth: { persistSession: false } }
);

const BASE = "https://sharehubnepal.com/live/api/v2/floorsheet";


// Utility to chunk arrays
function chunk(arr, size = 200) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}


// Fetch floorsheet for a single day
async function fetchDay(date) {
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages) {
    try {
      const url = `${BASE}?Size=100&date=${date}&page=${page}`;
      const res = await axios.get(url, { timeout: 15000 });
      const data = res.data?.data;

      if (!data) break;
      totalPages = data.totalPages;

      const rows = data.content.map(r => ({
        d: date,
        t: r.tradeTime.split("T")[1].split(".")[0],
        s: r.symbol,
        i: r.contractId,
        b: r.buyerMemberId,
        se: r.sellerMemberId,
        q: r.contractQuantity,
        p: r.contractRate,
        a: r.contractAmount
      }));

      if (!rows.length) {
        console.log("No floorsheet data for", date, "page", page);
        break;
      }

      // Upsert in chunks to avoid request size issues
      for (const batch of chunk(rows, 200)) {
        const { error } = await supabase
          .from("floorsheet")
          .upsert(batch, {
            onConflict: "i",       // primary key conflict
            ignoreDuplicates: true,
            returning: "minimal"
          });

        if (error && !error.message.includes("duplicate")) {
          console.error("Upsert error:", error.message);
        }
      }

      page++;
    } catch (err) {
      console.error("Error fetching date", date, "page", page, err.message);
      break;
    }
  }
}


// Get last N trading days to fill 21-day window
// Only missing floorsheet days are returned
async function getMissingTradingDates(targetDays = 21) {
  // Fetch existing trading days from the database using RPC
  const { data: existingDates, error } = await supabase
    .rpc("get_distinct_floorsheet_dates"); 

  if (error) {
    console.error("Error fetching existing dates:", error.message);
    return [];
  }

  const existingSet = new Set(existingDates.map(r => r.d));

  // Generate past calendar days until we have enough missing days
  const missingDates = [];
  let i = 0;
  const today = new Date();

  while (missingDates.length < targetDays) {
    const d = new Date();
    d.setDate(today.getDate() - i);
    const iso = d.toISOString().slice(0, 10);

    // Skip if already in table
    if (!existingSet.has(iso)) missingDates.push(iso);

    i++;

    // Safety: stop after 60 days to prevent infinite loops
    if (i > 60) break;
  }

  // Return oldest → newest
  return missingDates.reverse();
}

// cleanup old rows >21 trading days
async function cleanupOldRows(targetDays = 21) {
  try {
    await supabase.rpc("cleanup_old_floorsheet"); 
  } catch (err) {
    console.error("Cleanup error:", err.message);
  }
}


// Main runner
async function run() {
  // Determine backfill or daily mode
  const BACKFILL = Number(process.env.BACKFILL || 0);

  let datesToFetch = [];

  if (BACKFILL > 0) {
    console.log("Backfill mode:", BACKFILL, "days");

    // Get missing trading dates to complete 21-day window
    datesToFetch = await getMissingTradingDates(BACKFILL);
  } else {
    console.log("Daily mode");
    // Always fetch today only
    const today = new Date().toISOString().slice(0, 10);
    datesToFetch = [today];
  }

  console.log("Fetching dates:", datesToFetch);

  // Fetch each date sequentially
  for (const d of datesToFetch) {
    console.log("Fetching floorsheet for:", d);
    await fetchDay(d);
  }

  // Cleanup old rows to keep only last 21 trading days
  await cleanupOldRows(21);

  console.log("Floorsheet fetch + cleanup complete");
}

run();
