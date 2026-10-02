const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const mongoose = require("mongoose");
const cors = require("cors");
require("dotenv").config();

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

const MONGO_URI = process.env.MONGO_URI || "mongodb://localhost:27017/syncscript";

mongoose
  .connect(MONGO_URI)
  .then(() => console.log("Connected to MongoDB Atlas"))
  .catch((err) => console.error("MongoDB connection failed:", err.message));

// Document Schema
const DocumentSchema = new mongoose.Schema({
  _id: String,
  data: Object,
});
const Document = mongoose.model("Document", DocumentSchema);

async function findOrCreateDocument(id) {
  if (!id) return;
  const doc = await Document.findById(id);
  if (doc) return doc;
  return await Document.create({ _id: id, data: "" });
}

// In-Memory Room States: docId -> { isLocked: boolean, hostKey: string }
const roomStates = new Map();

// Rate-limiting and size thresholds
const socketRateLimits = new Map();
const MAX_PAYLOAD_SIZE = 1 * 1024 * 1024; // 1 MB limit
const MAX_MESSAGES_PER_SEC = 30;

function isRateLimited(socketId) {
  const now = Date.now();
  const record = socketRateLimits.get(socketId) || { count: 0, lastReset: now };

  if (now - record.lastReset > 1000) {
    record.count = 1;
    record.lastReset = now;
  } else {
    record.count += 1;
  }

  socketRateLimits.set(socketId, record);
  return record.count > MAX_MESSAGES_PER_SEC;
}

io.on("connection", (socket) => {
  socket.on("get-document", async ({ docId, hostKey }) => {
    socket.join(docId);
    socket.currentRoom = docId;

    let room = roomStates.get(docId);
    if (!room) {
      const assignedKey = hostKey || require("crypto").randomUUID();
      room = { isLocked: false, hostKey: assignedKey };
      roomStates.set(docId, room);
      socket.isHost = true;
      socket.hostKey = assignedKey;
    } else {
      const isHost = Boolean(hostKey && room.hostKey === hostKey);
      socket.isHost = isHost;
      socket.hostKey = hostKey;
    }

    const document = await findOrCreateDocument(docId);
    socket.emit("load-document", document.data);

    socket.emit("room-init", {
      isHost: socket.isHost,
      isLocked: room.isLocked,
      assignedHostKey: socket.isHost ? room.hostKey : null,
    });

    const count = io.sockets.adapter.rooms.get(docId)?.size || 1;
    io.to(docId).emit("user-count", count);
  });

  socket.on("toggle-lock", ({ docId, hostKey }) => {
    const room = roomStates.get(docId);
    if (!room || room.hostKey !== hostKey) return;

    room.isLocked = !room.isLocked;
    io.to(docId).emit("lock-updated", room.isLocked);
  });

  socket.on("cursor-move", (data) => {
    if (socket.currentRoom) {
      socket.broadcast.to(socket.currentRoom).emit("cursor-update", {
        socketId: socket.id,
        ...data,
      });
    }
  });

  socket.on("send-changes", (incomingData) => {
    if (typeof incomingData !== "string") return;
    if (Buffer.byteLength(incomingData, "utf8") > MAX_PAYLOAD_SIZE) return;
    if (isRateLimited(socket.id)) return;

    const room = roomStates.get(socket.currentRoom);
    if (room?.isLocked && !socket.isHost) return;

    socket.broadcast.to(socket.currentRoom).emit("receive-changes", incomingData);
  });

  socket.on("save-document", async (documentData) => {
    if (typeof documentData !== "string") return;
    if (Buffer.byteLength(documentData, "utf8") > MAX_PAYLOAD_SIZE) return;

    const room = roomStates.get(socket.currentRoom);
    if (room?.isLocked && !socket.isHost) return;

    try {
      await Document.findByIdAndUpdate(socket.currentRoom, { data: documentData });
    } catch (err) {
      console.error("Save failed:", err.message);
    }
  });

  socket.on("disconnect", () => {
    socketRateLimits.delete(socket.id);
    if (socket.currentRoom) {
      socket.broadcast.to(socket.currentRoom).emit("cursor-remove", socket.id);
      const count = io.sockets.adapter.rooms.get(socket.currentRoom)?.size || 0;
      io.to(socket.currentRoom).emit("user-count", count);
    }
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`SyncScript server listening on port ${PORT}`);
});