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

// In-Memory Room States: docId -> { isLocked: boolean, hostKey: string }
const roomStates = new Map();

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  // Safe handler: supports both { docId, hostKey } AND plain string docId
  socket.on("get-document", async (payload) => {
    const docId = typeof payload === "object" && payload !== null ? payload.docId : payload;
    const incomingHostKey = typeof payload === "object" && payload !== null ? payload.hostKey : null;

    if (!docId) return;

    socket.join(docId);
    socket.currentRoom = docId;

    let room = roomStates.get(docId);

    if (!room) {
      // First person to open this room becomes Host
      const assignedKey = incomingHostKey || require("crypto").randomUUID();
      room = { isLocked: false, hostKey: assignedKey };
      roomStates.set(docId, room);
      socket.isHost = true;
      socket.hostKey = assignedKey;
    } else {
      // Joining socket is host ONLY if their hostKey matches the existing room's hostKey
      const isMatch = Boolean(incomingHostKey && room.hostKey === incomingHostKey);
      socket.isHost = isMatch;
      socket.hostKey = isMatch ? incomingHostKey : null;
    }

    const document = await findOrCreateDocument(docId);
    socket.emit("load-document", document ? document.data : "");

    // Send host role and lock state
    socket.emit("room-init", {
      isHost: socket.isHost,
      isLocked: room.isLocked,
      assignedHostKey: socket.isHost ? room.hostKey : null,
    });

    // Notify all devices in this room of the updated participant count
    const count = io.sockets.adapter.rooms.get(docId)?.size || 1;
    io.to(docId).emit("user-count", count);
  });

  socket.on("toggle-lock", (payload) => {
    const docId = typeof payload === "object" ? payload.docId : socket.currentRoom;
    const hostKey = typeof payload === "object" ? payload.hostKey : socket.hostKey;

    const room = roomStates.get(docId);
    if (!room || room.hostKey !== hostKey) return;

    room.isLocked = !room.isLocked;
    io.to(docId).emit("lock-updated", room.isLocked);
  });

  socket.on("send-changes", (incomingData) => {
    if (!socket.currentRoom || typeof incomingData !== "string") return;

    const room = roomStates.get(socket.currentRoom);
    if (room?.isLocked && !socket.isHost) return; // Block changes if room is locked and user is not host

    socket.broadcast.to(socket.currentRoom).emit("receive-changes", incomingData);
  });

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