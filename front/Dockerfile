# Use an alpine image for a lighter footprint
FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json /app/
RUN npm install -g npm@10.9.0
RUN npm install --production

COPY . /app
RUN npm run build

# Use an official serve image or use the node image to serve static files
EXPOSE 8080
CMD ["npx", "serve", "-s", "build", "-l", "8080"]
