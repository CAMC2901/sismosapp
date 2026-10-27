# ==============================================================================
# web/Dockerfile — Compilación React + TypeScript + Vite y servido con Nginx
# Versiones fijadas: node:20-bookworm-slim (build) y nginx:1.27-alpine (runtime).
# Ejecución sin privilegios root en puerto 8080 (mapeado a 3000 en host).
# ==============================================================================
FROM node:20-bookworm-slim AS builder

WORKDIR /app

COPY package.json ./
RUN npm install --no-audit --no-fund

COPY . .
RUN npm run build

FROM nginx:1.27-alpine

RUN apk add --no-cache curl && \
    chown -R nginx:nginx /usr/share/nginx/html && \
    chown -R nginx:nginx /var/cache/nginx && \
    chown -R nginx:nginx /var/log/nginx && \
    chown -R nginx:nginx /etc/nginx/conf.d && \
    touch /var/run/nginx.pid && \
    chown -R nginx:nginx /var/run/nginx.pid

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist /usr/share/nginx/html

USER nginx

EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]
