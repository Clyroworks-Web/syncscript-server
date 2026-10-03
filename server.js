const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const mongoose = require("mongoose");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());

// Render provides the PORT dynamically via environment variables
const PORT = process.env.PORT || 5000;
const server = http.createServer(app);

// Configure Socket.io with open CORS
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

// MongoDB Connection
const MONGO_URI = process.env.MONGO_URI || "mongodb://127.0.0.1:27017/syncscript";

mongoose
  .connect(MONGO_URI)
  .then(() => console.log("Connected to MongoDB Atlas"))
  .catch((err) => console.error("MongoDB Connection Error:", err.message));

// Document Schema
const DocumentSchema = new mongoose.Schema({
  _id: String,
  data: Object,
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

// In-memory room manager: docId -> { isLocked: boolean, hostKey: string }
const roomStates = new Map();

// Root route for health check
app.get("/", (req, res) => {
  res.send("SyncScript server is healthy and running!");
});

// Socket handlers
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
      room = { isLocked: false, hostKey: assignedKey };
      roomStates.set(docId, room);
      socket.isHost = true;
      socket.hostKey = assignedKey;
    } else {
      const isMatch = Boolean(incomingHostKey && room.hostKey === incomingHostKey);
      socket.isHost = isMatch;
      socket.hostKey = isMatch ? incomingHostKey : null;
    }

    const document = await findOrCreateDocument(docId);
    socket.emit("load-document", document ? document.data : "");

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

  socket.on("save-document", async (data) => {
    if (!socket.currentRoom) return;
    const room = roomStates.get(socket.currentRoom);
    if (room?.isLocked && !socket.isHost) return;

    try {
      await Document.findByIdAndUpdate(socket.currentRoom, { data });
    } catch (err) {
      console.error("Save error:", err.message);
    }
  });

  socket.on("disconnect", () => {
    if (socket.currentRoom) {
      io.to(socket.currentRoom).emit("cursor-remove", socket.id);
      const count = io.sockets.adapter.rooms.get(socket.currentRoom)?.size || 0;
      io.to(socket.currentRoom).emit("user-count", count);
    }
  });
});

server.listen(PORT, () => {
  console.log(`SyncScript server active on port ${PORT}`);
});