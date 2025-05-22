import http from 'k6/http';
import { check } from 'k6'; // Removed sleep from imports
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Read the binary data from the file in the init context
const data = open('test_payload.bin', 'b');

export const options = {
    vus: 10, // Number of virtual users
    duration: '30s', // Duration of the test
    thresholds: {
        http_req_failed: ['rate<0.01'], // http errors should be less than 1%
        http_req_duration: ['p(95)<200'], // 95% of requests should be below 200ms
    },
};

const BASE_URL = 'http://localhost:3000/blob';

export default function () {
    const key = randomString(10); // Generate a random key for each request

    // 1. Set (POST) a blob
    const postRes = http.post(`${BASE_URL}/${key}`, data, {
        headers: { 'Content-Type': 'application/octet-stream' },
    });
    check(postRes, {
        'POST status was 201 (Created)': (r) => r.status === 201,
    });
    // http.get(`${BASE_URL}/${key}`, {
    //     headers: { 'Content-Type': 'application/octet-stream' },
    // });
}
