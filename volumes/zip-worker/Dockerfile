# ---------------------------
# Build stage
# ---------------------------
FROM node:26-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json index.ts utils.ts ./
RUN npx tsc


# ---------------------------
# Runtime stage
# ---------------------------
FROM node:26-alpine

WORKDIR /app

# Install only prod deps
COPY package*.json ./
# RUN npm ci --omit=dev
RUN npm ci

# Copy compiled JS
COPY --from=builder /app/dist ./dist

ENV NODE_ENV=production
ENV NODE_OPTIONS="--max-old-space-size=256"

CMD ["node", "dist/index.js"]