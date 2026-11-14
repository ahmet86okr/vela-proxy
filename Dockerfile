FROM node:22-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY index.js ./
ENV PORT=7860
EXPOSE 7860
CMD ["node", "index.js"]
