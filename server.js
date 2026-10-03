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

// 1. Initialize the in-memory store for active rooms
// Maps: docId -> { isLocked: boolean, hostKey: string }
const roomStates = new Map();

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  // 2. Safe Room Joining & Host Resolution
  socket.on("get-document", async (payload) => {
    // Accepts either { docId, hostKey } OR a plain string docId
    const docId = typeof payload === "object" && payload !== null ? payload.docId : payload;
    const incomingHostKey = typeof payload === "object" && payload !== null ? payload.hostKey : null;

    if (!docId) return;

    socket.join(docId);
    socket.currentRoom = docId;

    let room = roomStates.get(docId);

    if (!room) {
      // First person to open this room becomes the Host
      const assignedKey = incomingHostKey || require("crypto").randomUUID();
      room = { isLocked: false, hostKey: assignedKey };
      roomStates.set(docId, room);
      socket.isHost = true;
      socket.hostKey = assignedKey;
    } else {
      // Other sockets are Hosts ONLY if their hostKey matches the room's hostKey
      const isMatch = Boolean(incomingHostKey && room.hostKey === incomingHostKey);
      socket.isHost = isMatch;
      socket.hostKey = isMatch ? incomingHostKey : null;
    }

    const document = await findOrCreateDocument(docId);
    socket.emit("load-document", document ? document.data : "");

    // Send the resolved role and lock state back to the client
    socket.emit("room-init", {
      isHost: socket.isHost,
      isLocked: room.isLocked,
      assignedHostKey: socket.isHost ? room.hostKey : null,
    });

    // Notify all participants in this room of the updated user count
    const count = io.sockets.adapter.rooms.get(docId)?.size || 1;
    io.to(docId).emit("user-count", count);
  });

  // 3. Host-Verified Presenter Lock Toggle
  socket.on("toggle-lock", (payload) => {
    const docId = typeof payload === "object" ? payload.docId : socket.currentRoom;
    const hostKey = typeof payload === "object" ? payload.hostKey : socket.hostKey;

    const room = roomStates.get(docId);
    if (!room || room.hostKey !== hostKey) return; // Ignore if not the host

    room.isLocked = !room.isLocked;
    io.to(docId).emit("lock-updated", room.isLocked);
  });

  // 4. Read-Only Protection on Typing
  socket.on("send-changes", (incomingData) => {
    if (!socket.currentRoom || typeof incomingData !== "string") return;

    const room = roomStates.get(socket.currentRoom);
    // Discard keystrokes if the room is locked and sender is not the host
    if (room?.isLocked && !socket.isHost) return;

    socket.broadcast.to(socket.currentRoom).emit("receive-changes", incomingData);
  });

  // 5. Read-Only Protection on Cloud Saving
  socket.on("save-document", async (documentData) => {
    if (!socket.currentRoom || typeof documentData !== "string") return;

    const room = roomStates.get(socket.currentRoom);
    if (room?.isLocked && !socket.isHost) return;

    try {
      await Document.findByIdAndUpdate(socket.currentRoom, { data: documentData });
    } catch (err) {
      console.error("Save error:", err.message);
    }
  });

  socket.on("cursor-move", (data) => {
    if (socket.currentRoom) {
      socket.broadcast.to(socket.currentRoom).emit("cursor-update", {
        socketId: socket.id,
        ...data,
      });
    }
  });

  socket.on("disconnect", () => {
    if (socket.currentRoom) {
      socket.broadcast.to(socket.currentRoom).emit("cursor-remove", socket.id);
      const count = io.sockets.adapter.rooms.get(socket.currentRoom)?.size || 0;
      io.to(socket.currentRoom).emit("user-count", count);
    }
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`SyncScript server active on port ${PORT}`);
});