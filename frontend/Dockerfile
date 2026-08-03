FROM node:20-alpine AS build
WORKDIR /app
ARG VITE_CESIUM_TOKEN
ARG VITE_CESIUM_ION_TOKEN
ARG VITE_LIVE_WS_COMPRESS=false
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_DATE=unknown
ENV VITE_CESIUM_TOKEN=$VITE_CESIUM_TOKEN
ENV VITE_CESIUM_ION_TOKEN=$VITE_CESIUM_ION_TOKEN
ENV VITE_LIVE_WS_COMPRESS=$VITE_LIVE_WS_COMPRESS
COPY package.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_DATE=unknown
LABEL com.algosphere.git.commit="$GIT_COMMIT" \
      com.algosphere.git.branch="$GIT_BRANCH" \
      com.algosphere.build.date="$BUILD_DATE" \
      org.opencontainers.image.revision="$GIT_COMMIT" \
      org.opencontainers.image.source="https://github.com/infojobjimmy-jpg/AlgoSphere-Capital-AI-Quant-Platform"
COPY nginx/default.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
