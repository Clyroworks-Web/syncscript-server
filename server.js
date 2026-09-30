require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const cors = require('cors');

const app = express();
const clientUrl = process.env.CLIENT_URL || 'http://localhost:5173';

app.use(cors({ origin: clientUrl }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: clientUrl,
    methods: ['GET', 'POST'],
  },
});

// Document Schema
const DocumentSchema = new mongoose.Schema({
  _id: String,
  data: { type: Object, default: '' },
  title: { type: String, default: 'Untitled Document' }
});

const Document = mongoose.model('Document', DocumentSchema);

const URI = process.env.MONGODB_URI;

const connectWithRetry = () => {
  console.log('Attempting to connect to MongoDB...');
  mongoose.connect(URI)
    .then(() => console.log('Successfully connected to MongoDB!'))
    .catch((err) => {
      console.log('Connection failed:', err.message);
      console.log('Retrying in 5 seconds...');
      setTimeout(connectWithRetry, 5000);
    });
};

connectWithRetry();

// Helper to find or initialize a document
async function findOrCreateDocument(id) {
  if (!id) return null;
  const doc = await Document.findById(id);
  if (doc) return doc;
  return await Document.create({ _id: id, data: '', title: 'Untitled Document' });
}

io.on('connection', (socket) => {
  console.log('A user connected:', socket.id);

  socket.on('get-document', async (documentId) => {
    const document = await findOrCreateDocument(documentId);
    socket.join(documentId);

    // Update active collaborator count
    const room = io.sockets.adapter.rooms.get(documentId);
    const numUsers = room ? room.size : 1;
    io.to(documentId).emit('update-user-count', numUsers);

    // Send existing text and title to newly joined client
    socket.emit('load-document', { data: document.data, title: document.title });

    // Broadcast text edits
    socket.on('send-changes', (delta) => {
      socket.to(documentId).emit('receive-changes', delta);
    });

    // Broadcast title edits and persist
    socket.on('send-title-change', async (newTitle) => {
      socket.to(documentId).emit('receive-title-change', newTitle);
      await Document.findByIdAndUpdate(documentId, { title: newTitle });
    });

    // Autosave text
    socket.on('save-document', async (data) => {
      await Document.findByIdAndUpdate(documentId, { data });
    });

    // Handle user leaving room
    socket.on('disconnecting', () => {
      socket.rooms.forEach((roomId) => {
        if (roomId !== socket.id) {
          const currentRoom = io.sockets.adapter.rooms.get(roomId);
          const remainingUsers = currentRoom ? currentRoom.size - 1 : 0;
          io.to(roomId).emit('update-user-count', remainingUsers);
        }
      });
    });
  });

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id);
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});