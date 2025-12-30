import axios from "axios";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  { auth: { persistSession: false } }
);

const BASE =
  "https://sharehubnepal.com/live/api/v2/floorsheet";

// Fetch one full day (all pages)
async function fetchDay(date) {
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages) {
    const url = `${BASE}?Size=100&date=${date}&page=${page}`;
    const res = await axios.get(url, { timeout: 15000 });

    const data = res.data?.data;
    if (!data) break;

    totalPages = data.totalPages;

    const rows = data.content.map(r => ({
      d: date,
      t: r.tradeTime.split("T")[1].split(".")[0], // HH:MM:SS only
      s: r.symbol,
      b: r.buyerMemberId,
      se: r.sellerMemberId,
      q: r.contractQuantity,
      p: r.contractRate,
      a: r.contractAmount
    }));

    if (rows.length) {
      const { error } = await supabase
        .from("floorsheet")
        .insert(rows, { returning: "minimal" });

      if (error) console.error(error.message);
    }

    page++;
  }
}

// Get dates back N days
function getDatesBack(days) {
  const dates = [];
  for (let i = 1; i <= days; i++) {   // start from yesterday
    const d = new Date();
    d.setDate(d.getDate() - i);
    dates.push(d.toISOString().slice(0, 10));
  }
  return dates;
}


// Main runner
async function run() {
  const DAYS = Number(process.env.BACKFILL || 1);
  const dates = getDatesBack(DAYS);

  for (const d of dates) {
    console.log("Fetching:", d);
    await fetchDay(d);
  }
}

run();
