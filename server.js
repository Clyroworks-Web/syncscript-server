require("dotenv").config();
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const mongoose = require("mongoose");
const cors = require("cors");

const app = express();
const server = http.createServer(app);

// Allow Vercel frontend and local development
const allowedOrigins = [
  "https://syncscript-client-sigma.vercel.app",
  "http://localhost:5173",
  "http://localhost:3000",
  process.env.CLIENT_URL,
].filter(Boolean);

app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(null, true); // Allow all during testing if needed
      }
    },
    credentials: true,
  })
);

app.use(express.json());

// 1. DATABASE CONNECTION
// Reads MONGODB_URI or MONGO_URI from Render environment variables
const MONGODB_URI =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  "mongodb://127.0.0.1:27017/syncscript";

mongoose
  .connect(MONGODB_URI)
  .then(() => console.log(" Connected to MongoDB Atlas successfully!"))
  .catch((err) => console.error(" MongoDB Connection Error:", err.message));

// Document Schema
const DocumentSchema = new mongoose.Schema(
  {
    _id: String,
    data: { type: String, default: "" },
  },
  { timestamps: true }
);

const Document = mongoose.model("Document", DocumentSchema);

// In-Memory Room Management for Live Sessions
const roomStates = new Map();

// Helper to get or create document from Atlas
async function findOrCreateDocument(id) {
  if (!id) return null;
  const doc = await Document.findById(id);
  if (doc) return doc;
  return await Document.create({ _id: id, data: "" });
}

// 2. SOCKET.IO SETUP
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true,
  },
});

io.on("connection", (socket) => {
  let currentDocId = null;

  // Initialize room and load document
  socket.on("get-document", async ({ docId, hostKey }) => {
    if (!docId) return;
    currentDocId = docId;

    socket.join(docId);

    // Initialize in-memory state for room if first time
    if (!roomStates.has(docId)) {
      roomStates.set(docId, {
        hostKey: hostKey || null,
        isLocked: false,
        isEnded: false,
        users: new Set(),
      });
    }

    const room = roomStates.get(docId);
    if (!room.hostKey && hostKey) {
      room.hostKey = hostKey;
    }

    room.users.add(socket.id);

    const isHost = Boolean(hostKey && room.hostKey === hostKey);

    // Send initial status back to caller
    socket.emit("room-init", {
      isHost,
      isLocked: room.isLocked,
      isEnded: room.isEnded,
    });

    // Broadcast updated user count
    io.to(docId).emit("user-count", room.users.size);

    // Load document content from database
    const document = await findOrCreateDocument(docId);
    if (document) {
      socket.emit("load-document", document.data);
    }
  });

  // Broadcast text changes
  socket.on("send-changes", (content) => {
    if (!currentDocId) return;
    const room = roomStates.get(currentDocId);
    if (room && room.isEnded) return;

    socket.to(currentDocId).emit("receive-changes", content);
  });

  // Save changes to MongoDB Atlas
  socket.on("save-document", async (content) => {
    if (!currentDocId) return;
    const room = roomStates.get(currentDocId);
    if (room && room.isEnded) return;

    try {
      await Document.findByIdAndUpdate(currentDocId, { data: content });
    } catch (err) {
      console.error("Error saving document:", err.message);
    }
  });

  // Live Multiplayer Cursors
  socket.on("cursor-move", ({ x, y, name, color }) => {
    if (!currentDocId) return;
    socket.to(currentDocId).emit("cursor-update", {
      socketId: socket.id,
      x,
      y,
      name,
      color,
    });
  });

  // Host Lock Toggle
  socket.on("toggle-lock", ({ docId, hostKey }) => {
    const room = roomStates.get(docId);
    if (room && room.hostKey === hostKey && !room.isEnded) {
      room.isLocked = !room.isLocked;
      io.to(docId).emit("lock-updated", room.isLocked);
    }
  });

  // Host Terminate Meeting
  socket.on("end-meeting", ({ docId, hostKey }) => {
    const room = roomStates.get(docId);
    if (room && room.hostKey === hostKey) {
      room.isEnded = true;
      room.isLocked = true;
      io.to(docId).emit("meeting-terminated");
    }
  });

  // Handle User Disconnect
  socket.on("disconnect", () => {
    if (currentDocId && roomStates.has(currentDocId)) {
      const room = roomStates.get(currentDocId);
      room.users.delete(socket.id);

      io.to(currentDocId).emit("cursor-remove", socket.id);
      io.to(currentDocId).emit("user-count", room.users.size);

      // Clean up empty room states
      if (room.users.size === 0) {
        roomStates.delete(currentDocId);
      }
    }
  });
});

// Basic Health Check Route
app.get("/", (req, res) => {
  res.send("SyncScript Backend Server is Running!");
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
  console.log(`SyncScript server active on port ${PORT}`);
});