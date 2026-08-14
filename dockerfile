
# Use Node 20 slim for a small, fast footprint
FROM node:20-slim

# Set Production Environment
ENV NODE_ENV=production
# Update PORT to 10000 to match your iNFLUENSA server.js
ENV PORT=10000
ENV NODE_OPTIONS="--max-old-space-size=2048"

# CRITICAL: Install system binaries for FFmpeg (Video Watermarking)
# and clean up to keep the Afro-Futurist grid lightweight
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Install dependencies
COPY package*.json ./

# CRITICAL FIX: 'npm install' ensures jimp and other neural logic deps are ready
RUN npm install --only=production

# Copy all protocol files
COPY . .

# Ensure the uploads folder exists with correct permissions for IP storage
RUN mkdir -p uploads && chmod -R 777 uploads

# Expose 10000 to allow M-Pesa callbacks and Neural Link access
EXPOSE 10000

# Updated Healthcheck to point to Port 10000
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD node -e "require('http').get('http://localhost:10000/api/stats', (r) => { \
    if (r.statusCode === 200) process.exit(0); \
    else process.exit(1); \
  }).on('error', () => process.exit(1))"

CMD [ "node", "server.js" ]

