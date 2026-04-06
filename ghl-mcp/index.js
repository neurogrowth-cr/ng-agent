#!/usr/bin/env node
/**
 * GoHighLevel MCP Server — NeuroGrowth
 * Exposes GHL pipeline, contact, and opportunity management as MCP tools.
 * Reads GHL_API_KEY and GHL_LOCATION_ID from the parent .env file.
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { createRequire } from "module";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";
import * as dotenv from "dotenv";

// Load .env from parent directory (ng-agent root)
const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, "../.env") });

const GHL_API_KEY = process.env.GHL_API_KEY;
const GHL_LOCATION_ID = process.env.GHL_LOCATION_ID;
const BASE_URL = "https://services.leadconnectorhq.com";
const GHL_VERSION = "2021-07-28";

if (!GHL_API_KEY || !GHL_LOCATION_ID) {
  console.error("ERROR: GHL_API_KEY and GHL_LOCATION_ID must be set in ../.env");
  process.exit(1);
}

// ─── GHL API Helper ────────────────────────────────────────────────────────────

async function ghlRequest(method, path, params = {}, body = null) {
  const url = new URL(`${BASE_URL}${path}`);
  if (method === "GET") {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, v);
    });
  }

  const options = {
    method,
    headers: {
      Authorization: `Bearer ${GHL_API_KEY}`,
      Version: GHL_VERSION,
      "Content-Type": "application/json",
    },
  };

  if (body) options.body = JSON.stringify(body);

  const res = await fetch(url.toString(), options);
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`GHL API error ${res.status}: ${text}`);
  }

  return text ? JSON.parse(text) : {};
}

// ─── MCP Server Setup ──────────────────────────────────────────────────────────

const server = new McpServer({
  name: "ghl-mcp-server",
  version: "1.0.0",
});

// ─── TOOL: Get Pipelines ───────────────────────────────────────────────────────

server.tool(
  "get_pipelines",
  "List all GoHighLevel pipelines and their stages for this location.",
  {},
  async () => {
    const data = await ghlRequest("GET", "/opportunities/pipelines", {
      locationId: GHL_LOCATION_ID,
    });
    const pipelines = data.pipelines || [];
    const summary = pipelines.map((p) => ({
      id: p.id,
      name: p.name,
      stages: (p.stages || []).map((s) => ({ id: s.id, name: s.name, position: s.position })),
    }));
    return {
      content: [{ type: "text", text: JSON.stringify(summary, null, 2) }],
    };
  }
);

// ─── TOOL: Get Opportunities in Stage ─────────────────────────────────────────

server.tool(
  "get_opportunities_in_stage",
  "Get all opportunities (leads) in a specific pipeline stage. Use get_pipelines first to find stage IDs.",
  {
    pipeline_id: z.string().describe("The pipeline ID"),
    stage_id: z.string().optional().describe("The stage ID to filter by (optional — omit to get all stages)"),
    limit: z.number().optional().default(100).describe("Max results (default 100, max 100)"),
    page: z.number().optional().default(1).describe("Page number for pagination"),
  },
  async ({ pipeline_id, stage_id, limit = 100, page = 1 }) => {
    const params = {
      location_id: GHL_LOCATION_ID,
      pipeline_id,
      limit,
      page,
    };
    if (stage_id) params.pipeline_stage_id = stage_id;

    const data = await ghlRequest("GET", "/opportunities/search", params);
    const opportunities = data.opportunities || [];

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(
            {
              total: data.meta?.total || opportunities.length,
              page: data.meta?.currentPage || page,
              opportunities: opportunities.map((o) => ({
                id: o.id,
                name: o.name,
                status: o.status,
                stage: o.pipelineStageId,
                stageName: o.pipelineStageName,
                contact: {
                  id: o.contact?.id,
                  name: o.contact?.name,
                  email: o.contact?.email,
                  phone: o.contact?.phone,
                },
                createdAt: o.createdAt,
                updatedAt: o.updatedAt,
              })),
            },
            null,
            2
          ),
        },
      ],
    };
  }
);

// ─── TOOL: Search Contacts ─────────────────────────────────────────────────────

server.tool(
  "search_contacts",
  "Search contacts in GoHighLevel by name, email, or phone number.",
  {
    query: z.string().describe("Search term (name, email, or phone)"),
    limit: z.number().optional().default(50).describe("Max results (default 50)"),
  },
  async ({ query, limit = 50 }) => {
    const data = await ghlRequest("GET", "/contacts/", {
      locationId: GHL_LOCATION_ID,
      query,
      limit,
    });
    const contacts = data.contacts || [];
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(
            contacts.map((c) => ({
              id: c.id,
              name: `${c.firstName || ""} ${c.lastName || ""}`.trim(),
              email: c.email,
              phone: c.phone,
              tags: c.tags,
              createdAt: c.dateAdded,
            })),
            null,
            2
          ),
        },
      ],
    };
  }
);

// ─── TOOL: Find Duplicate Contacts ────────────────────────────────────────────

server.tool(
  "find_duplicate_contacts",
  "Scan contacts in a pipeline stage and detect duplicates by email or phone. Returns grouped duplicates ready for review.",
  {
    pipeline_id: z.string().describe("The pipeline ID to scan"),
    stage_id: z.string().optional().describe("Limit scan to a specific stage ID (e.g. 'New Lead' stage)"),
    match_by: z.enum(["email", "phone", "both"]).optional().default("both").describe("How to detect duplicates"),
  },
  async ({ pipeline_id, stage_id, match_by = "both" }) => {
    // Fetch all opportunities in the stage (paginate through all)
    let allOpps = [];
    let page = 1;
    let total = Infinity;

    while (allOpps.length < total) {
      const params = {
        location_id: GHL_LOCATION_ID,
        pipeline_id,
        limit: 100,
        page,
      };
      if (stage_id) params.pipeline_stage_id = stage_id;

      const data = await ghlRequest("GET", "/opportunities/search", params);
      const opps = data.opportunities || [];
      total = data.meta?.total ?? opps.length;
      allOpps = allOpps.concat(opps);
      if (opps.length < 100) break;
      page++;
    }

    // Group by email and/or phone
    const emailMap = {};
    const phoneMap = {};

    for (const opp of allOpps) {
      const email = opp.contact?.email?.toLowerCase().trim();
      const phone = opp.contact?.phone?.replace(/\D/g, "");
      const entry = {
        opportunityId: opp.id,
        opportunityName: opp.name,
        contactId: opp.contact?.id,
        contactName: opp.contact?.name,
        email: opp.contact?.email,
        phone: opp.contact?.phone,
        stage: opp.pipelineStageName,
        createdAt: opp.createdAt,
      };

      if ((match_by === "email" || match_by === "both") && email) {
        if (!emailMap[email]) emailMap[email] = [];
        emailMap[email].push(entry);
      }
      if ((match_by === "phone" || match_by === "both") && phone && phone.length >= 7) {
        if (!phoneMap[phone]) phoneMap[phone] = [];
        phoneMap[phone].push(entry);
      }
    }

    // Filter to only groups with duplicates
    const emailDupes = Object.entries(emailMap)
      .filter(([, group]) => group.length > 1)
      .map(([email, group]) => ({ duplicateKey: email, matchType: "email", count: group.length, records: group }));

    const phoneDupes = Object.entries(phoneMap)
      .filter(([, group]) => group.length > 1)
      .map(([phone, group]) => ({ duplicateKey: phone, matchType: "phone", count: group.length, records: group }));

    const allDupes = [...emailDupes, ...phoneDupes];

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(
            {
              totalOpportunitiesScanned: allOpps.length,
              duplicateGroupsFound: allDupes.length,
              duplicates: allDupes,
            },
            null,
            2
          ),
        },
      ],
    };
  }
);

// ─── TOOL: Get Contact Details ─────────────────────────────────────────────────

server.tool(
  "get_contact",
  "Get full details for a single contact by their contact ID.",
  {
    contact_id: z.string().describe("The GHL contact ID"),
  },
  async ({ contact_id }) => {
    const data = await ghlRequest("GET", `/contacts/${contact_id}`);
    return {
      content: [{ type: "text", text: JSON.stringify(data.contact || data, null, 2) }],
    };
  }
);

// ─── TOOL: Delete Opportunity ──────────────────────────────────────────────────

server.tool(
  "delete_opportunity",
  "Delete a specific opportunity (pipeline card) by its ID. This removes it from the pipeline but does NOT delete the underlying contact.",
  {
    opportunity_id: z.string().describe("The opportunity ID to delete"),
  },
  async ({ opportunity_id }) => {
    await ghlRequest("DELETE", `/opportunities/${opportunity_id}`);
    return {
      content: [{ type: "text", text: `Opportunity ${opportunity_id} deleted successfully.` }],
    };
  }
);

// ─── TOOL: Delete Contact ──────────────────────────────────────────────────────

server.tool(
  "delete_contact",
  "Permanently delete a contact by their contact ID. This removes them from all pipelines and the CRM. Use with caution.",
  {
    contact_id: z.string().describe("The GHL contact ID to permanently delete"),
  },
  async ({ contact_id }) => {
    await ghlRequest("DELETE", `/contacts/${contact_id}`);
    return {
      content: [{ type: "text", text: `Contact ${contact_id} permanently deleted.` }],
    };
  }
);

// ─── TOOL: Update Opportunity Stage ───────────────────────────────────────────

server.tool(
  "update_opportunity_stage",
  "Move an opportunity to a different pipeline stage.",
  {
    opportunity_id: z.string().describe("The opportunity ID"),
    stage_id: z.string().describe("The target stage ID to move it to"),
    status: z.enum(["open", "won", "lost", "abandoned"]).optional().describe("Optionally update the opportunity status"),
  },
  async ({ opportunity_id, stage_id, status }) => {
    const body = { pipelineStageId: stage_id };
    if (status) body.status = status;
    const data = await ghlRequest("PUT", `/opportunities/${opportunity_id}`, {}, body);
    return {
      content: [{ type: "text", text: `Opportunity updated successfully.\n${JSON.stringify(data, null, 2)}` }],
    };
  }
);

// ─── Start Server ──────────────────────────────────────────────────────────────

const transport = new StdioServerTransport();
await server.connect(transport);
console.error("GHL MCP Server running — connected to location:", GHL_LOCATION_ID);
