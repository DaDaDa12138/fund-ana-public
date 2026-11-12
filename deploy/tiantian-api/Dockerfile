FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32

WORKDIR /app
ENV NODE_ENV=production PORT=3000 TZ=Asia/Shanghai

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund && npm cache clean --force

COPY upstream ./upstream
COPY server.js upstream.json ./
COPY tests ./tests

USER node
EXPOSE 3000
CMD ["node", "server.js"]
