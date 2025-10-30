# Webhook Implementation for Replicate AI Video Generation

This document describes the webhook-based implementation for Replicate AI video generation, replacing the previous polling mechanism.

## Overview

The webhook implementation provides:
- Real-time notifications from Replicate when video generation completes
- Reduced server resource usage compared to polling
- Better reliability and error handling
- Secure webhook signature verification

## Architecture

### Current Polling Flow (Legacy)
1. Request submitted to Replicate API
2. Service polls Replicate API every 10 seconds for up to 20 minutes
3. Status checked until completion or timeout

### New Webhook Flow
1. Request submitted to Replicate API with webhook URL
2. QStash processing detects webhook-enabled request and waits (returns "pending")
3. Replicate calls our webhook endpoint when status changes
4. Webhook processes completion and updates request status
5. No duplicate callback execution - prevents double processing

## Configuration

### Environment Variables

The following environment variables are required for webhook functionality:

```bash
# Enable webhook functionality (set to "true" to enable)
ENABLE_REPLICATE_WEBHOOKS=true

# Replicate webhook signing secret for signature verification
REPLICATE_WEBHOOK_SIGNING_SECRET=your_replicate_signing_secret

# Base URL for webhook endpoint (already configured)
OFF_CHAIN_AGENT_URL=https://your-domain.com
```

### Replicate Configuration

In your Replicate account:
1. Generate a webhook signing secret
2. Set the signing secret in the environment variable
3. The webhook URL will be automatically set to `{OFF_CHAIN_AGENT_URL}/replicate/webhook`

## Endpoints

### Webhook Endpoint
- **URL**: `POST /replicate/webhook`
- **Purpose**: Receives webhook notifications from Replicate
- **Authentication**: HMAC signature verification
- **Headers Required**:
  - `webhook-id`: Unique webhook message ID
  - `webhook-timestamp`: Unix timestamp of the webhook
  - `webhook-signature`: HMAC signature (v1=<signature>)

## Implementation Details

### Webhook Security
- HMAC-SHA256 signature verification using Replicate signing secret
- Timestamp validation (5-minute window) to prevent replay attacks
- Constant-time signature comparison to prevent timing attacks

### Backward Compatibility
- Polling mechanism remains as fallback when webhooks are disabled
- Environment flag `ENABLE_REPLICATE_WEBHOOKS` controls the behavior
- Both flows use the same callback processing system

### Metadata Tracking
Webhook requests include metadata to track internal state:
```json
{
  "user_principal": "user_id",
  "request_key_principal": "principal",
  "request_key_counter": 123,
  "property": "VIDEOGEN",
  "deducted_amount": 100,
  "token_type": "Sats"
}
```

## Models Supporting Webhooks

Currently implemented for:
- ✅ WAN 2.5 (`wan2_5.rs`)
- ✅ WAN 2.5 Fast (`wan2_5_fast.rs`)
- ⚠️ LumaLabs (still uses polling - future enhancement)
- ⚠️ IntTest (still uses polling - future enhancement)

## Migration Path

1. **Phase 1**: Deploy webhook implementation with `ENABLE_REPLICATE_WEBHOOKS=false`
2. **Phase 2**: Set up Replicate webhook configuration and signing secret
3. **Phase 3**: Enable webhooks with `ENABLE_REPLICATE_WEBHOOKS=true`
4. **Phase 4**: Monitor and validate webhook functionality
5. **Phase 5**: Eventually remove polling code (future release)

## Error Handling

### Webhook Failures
- Invalid signatures are rejected with 401 status
- Missing headers return 400 status
- Parse errors return 400 status
- Timestamp validation prevents replay attacks

### Fallback Behavior
- When `ENABLE_REPLICATE_WEBHOOKS=false`, uses polling
- Individual requests can fail back to polling if webhook setup fails
- Webhook processing errors are logged but don't break the flow

### Double Execution Prevention
- QStash processor detects webhook-enabled requests with empty video URLs
- Returns "pending" status instead of triggering callback immediately
- Only webhook completion triggers the actual callback processing
- Prevents race conditions and duplicate status updates

## Testing

### Unit Tests
Tests cover:
- Webhook URL generation
- Status conversion from Replicate format
- Metadata serialization/deserialization
- Signature verification logic

### Integration Testing
For testing webhook functionality:
1. Use webhook testing tools like ngrok for local development
2. Set up test signing secrets
3. Send test webhook payloads to verify processing

## Monitoring

### Logs
- Webhook receipt and processing are logged at INFO level
- Signature verification failures logged at ERROR level
- Status transitions logged for debugging

### Metrics
- Success/failure rates for webhook processing
- Comparison with polling performance
- Error rates and types

## Benefits

### Performance
- Eliminates 10-second polling intervals
- Reduces API calls to Replicate
- Faster response times for users

### Resource Usage
- Lower CPU usage (no polling loops)
- Reduced memory usage for long-running requests
- Better scalability

### Reliability
- Real-time notifications
- No timeout issues with long-running generations
- Better error visibility

## Security Considerations

- Webhook signatures are verified using HMAC-SHA256
- Timestamp validation prevents replay attacks
- Signing secrets should be rotated regularly
- Webhook endpoint is separate from public API endpoints

## Future Enhancements

1. **Additional Models**: Add webhook support to LumaLabs and other providers
2. **Enhanced Monitoring**: Add Prometheus metrics for webhook performance
3. **Retry Logic**: Implement retry mechanism for webhook processing failures
4. **Rate Limiting**: Add rate limiting to webhook endpoint
5. **Webhook Management**: Add API endpoints to manage webhook configuration