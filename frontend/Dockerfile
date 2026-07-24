FROM node:20-alpine AS build
WORKDIR /app
ARG VITE_CESIUM_TOKEN
ARG VITE_CESIUM_ION_TOKEN
ARG VITE_LIVE_WS_COMPRESS=false
ENV VITE_CESIUM_TOKEN=$VITE_CESIUM_TOKEN
ENV VITE_CESIUM_ION_TOKEN=$VITE_CESIUM_ION_TOKEN
ENV VITE_LIVE_WS_COMPRESS=$VITE_LIVE_WS_COMPRESS
COPY package.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx/default.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
