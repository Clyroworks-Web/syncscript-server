const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const mongoose = require("mongoose");
const cors = require("cors");
const helmet = require("helmet");
const crypto = require("crypto");
require("dotenv").config();

const app = express();

// Security Headers: blocks clickjacking, MIME sniffing, and removes X-Powered-By
app.use(
  helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false,
  })
);

// Restricted CORS: only allows authorized production and local origins
const allowedOrigins = [
  "https://syncscript-client-sigma.vercel.app",
  "http://localhost:5173",
  "http://localhost:3000",
];

app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin || allowedOrigins.includes(origin)) {
        return callback(null, true);
      }
      return callback(new Error("CORS policy violation: Unauthorized origin"));
    },
    credentials: true,
  })
);

const PORT = process.env.PORT || 5000;
const server = http.createServer(app);

// Socket.io configuration matching CORS policy
const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"],
    credentials: true,
  },
});

// Root Health Check Route
app.get("/", (req, res) => {
  res.send("SyncScript server is healthy and running!");
});

// MongoDB Atlas Connection
const MONGO_URI = process.env.MONGO_URI || "mongodb://127.0.0.1:27017/syncscript";

mongoose
  .connect(MONGO_URI)
  .then(() => console.log("Connected to MongoDB Atlas"))
  .catch((err) => console.error("MongoDB Connection Error:", err.message));

// Document Schema with 30-Day Auto-Expiration (TTL)
const DocumentSchema = new mongoose.Schema({
  _id: String,
  data: { type: String, default: "" },
  updatedAt: {
    type: Date,
    default: Date.now,
    expires: 2592000, // 30 days in seconds (30 * 24 * 60 * 60)
  },
});

const Document = mongoose.model("Document", DocumentSchema);

async function findOrCreateDocument(id) {
  if (!id) return null;
  try {
    const doc = await Document.findById(id);
    if (doc) return doc;
    return await Document.create({ _id: id, data: "" });
  } catch (err) {
    console.error("Document lookup error:", err.message);
    return null;
  }
}

// In-memory room manager: docId -> { isLocked: boolean, hostKey: string, data: string }
const roomStates = new Map();

// Trailing-edge database flush timers: docId -> timeoutId
const saveTimeouts = new Map();

io.on("connection", (socket) => {
  socket.on("get-document", async (payload) => {
    const docId = typeof payload === "object" && payload !== null ? payload.docId : payload;
    const incomingHostKey = typeof payload === "object" && payload !== null ? payload.hostKey : null;

    if (!docId) return;

    socket.join(docId);
    socket.currentRoom = docId;

    let room = roomStates.get(docId);

    if (!room) {
      const assignedKey = incomingHostKey || crypto.randomUUID();
      const document = await findOrCreateDocument(docId);
      const initialData = document ? document.data : "";

      room = {
        isLocked: false,
        hostKey: assignedKey,
        data: initialData,
      };
      roomStates.set(docId, room);
      socket.isHost = true;
      socket.hostKey = assignedKey;
    } else {
      const isMatch = Boolean(incomingHostKey && room.hostKey === incomingHostKey);
      socket.isHost = isMatch;
      socket.hostKey = isMatch ? incomingHostKey : null;
    }

    // Immediately send current buffer to joining/reconnecting client
    socket.emit("load-document", room.data || "");

    socket.emit("room-init", {
      isHost: socket.isHost,
      isLocked: room.isLocked,
      assignedHostKey: socket.isHost ? room.hostKey : null,
    });

    const count = io.sockets.adapter.rooms.get(docId)?.size || 1;
    io.to(docId).emit("user-count", count);
  });

  socket.on("send-changes", (delta) => {
    if (!socket.currentRoom) return;
    const room = roomStates.get(socket.currentRoom);
    if (room?.isLocked && !socket.isHost) return;

    if (room && typeof delta === "string") {
      room.data = delta;
    }

    socket.broadcast.to(socket.currentRoom).emit("receive-changes", delta);
  });

  socket.on("cursor-move", (data) => {
    if (!socket.currentRoom) return;
    socket.broadcast.to(socket.currentRoom).emit("cursor-update", {
      socketId: socket.id,
      ...data,
    });
  });

  socket.on("toggle-lock", (payload) => {
    const docId = typeof payload === "object" ? payload.docId : socket.currentRoom;
    const hostKey = typeof payload === "object" ? payload.hostKey : socket.hostKey;

    const room = roomStates.get(docId);
    if (!room || room.hostKey !== hostKey) return;

    room.isLocked = !room.isLocked;
    io.to(docId).emit("lock-updated", room.isLocked);
  });

  // Host Kill-Switch: Terminates session, evicts RAM, wipes doc from MongoDB
  socket.on("end-meeting", async (payload) => {
    const docId = typeof payload === "object" ? payload.docId : socket.currentRoom;
    const hostKey = typeof payload === "object" ? payload.hostKey : socket.hostKey;

    const room = roomStates.get(docId);
    if (!room || room.hostKey !== hostKey) return;

    // 1. Notify all connected clients in the room
    io.to(docId).emit("meeting-terminated");

    // 2. Disconnect everyone from the socket room
    io.in(docId).socketsLeave(docId);

    // 3. Clear timers and purge from memory
    if (saveTimeouts.has(docId)) {
      clearTimeout(saveTimeouts.get(docId));
      saveTimeouts.delete(docId);
    }
    roomStates.delete(docId);

    // 4. Wipe document from MongoDB so old link is unusable
    try {
      await Document.findByIdAndDelete(docId);
    } catch (err) {
      console.error("Error wiping ended meeting document:", err.message);
    }
  });

  // Debounced database write: flushes after 1000ms idle
  socket.on("save-document", (data) => {
    if (!socket.currentRoom) return;
    const docId = socket.currentRoom;
    const room = roomStates.get(docId);

    if (room?.isLocked && !socket.isHost) return;

    if (room && typeof data === "string") {
      room.data = data;
    }

    if (saveTimeouts.has(docId)) {
      clearTimeout(saveTimeouts.get(docId));
    }

    const timeoutId = setTimeout(async () => {
      saveTimeouts.delete(docId);
      const activeRoom = roomStates.get(docId);
      if (!activeRoom) return;

      try {
        await Document.findByIdAndUpdate(
          docId,
          { data: activeRoom.data, updatedAt: new Date() },
          { upsert: true }
        );
      } catch (err) {
        console.error("Database flush error:", err.message);
      }
    }, 1000);

    saveTimeouts.set(docId, timeoutId);
  });

  // Disconnection handler: flushes final changes and clears empty rooms
  socket.on("disconnect", async () => {
    const docId = socket.currentRoom;
    if (!docId) return;

    io.to(docId).emit("cursor-remove", socket.id);
    const roomOccupants = io.sockets.adapter.rooms.get(docId)?.size || 0;
    io.to(docId).emit("user-count", roomOccupants);

    if (roomOccupants === 0) {
      if (saveTimeouts.has(docId)) {
        clearTimeout(saveTimeouts.get(docId));
        saveTimeouts.delete(docId);
      }

      const room = roomStates.get(docId);
      if (room?.data) {
        try {
          await Document.findByIdAndUpdate(
            docId,
            { data: room.data, updatedAt: new Date() },
            { upsert: true }
          );
        } catch (err) {
          console.error("Final exit save error:", err.message);
        }
      }

      roomStates.delete(docId);
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`SyncScript server active on port ${PORT}`);
});