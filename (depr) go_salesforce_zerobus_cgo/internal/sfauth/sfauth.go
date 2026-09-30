package sfauth

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/databricks-solutions/go-salesforce-zerobus/internal/config"
	"google.golang.org/grpc/metadata"
)

// Credentials holds Salesforce session information for gRPC metadata.
type Credentials struct {
	AccessToken string
	InstanceURL string
	TenantID    string
}

// GRPCMetadata returns the metadata headers required for Salesforce Pub/Sub API calls.
func (c *Credentials) GRPCMetadata() metadata.MD {
	return metadata.Pairs(
		"accesstoken", c.AccessToken,
		"instanceurl", c.InstanceURL,
		"tenantid", c.TenantID,
	)
}

// Authenticate performs Salesforce authentication using OAuth or SOAP based on config.
func Authenticate(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Credentials, error) {
	switch cfg.GetAuthMode() {
	case config.AuthOAuth:
		logger.Info("Authenticating with Salesforce via OAuth 2.0 Client Credentials")
		return authenticateOAuth(ctx, cfg.SFInstanceURL, cfg.SFClientID, cfg.SFClientSecret)
	case config.AuthSOAP:
		logger.Info("Authenticating with Salesforce via SOAP Login")
		return authenticateSOAP(ctx, cfg.SFInstanceURL, cfg.SFUsername, cfg.SFPassword, cfg.APIVersion)
	default:
		return nil, fmt.Errorf("no valid authentication credentials provided")
	}
}

func authenticateOAuth(ctx context.Context, instanceURL, clientID, clientSecret string) (*Credentials, error) {
	tokenURL := strings.TrimRight(instanceURL, "/") + "/services/oauth2/token"

	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("creating OAuth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("OAuth token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading OAuth response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OAuth token request returned %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		InstanceURL string `json:"instance_url"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("parsing OAuth response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return nil, fmt.Errorf("OAuth response missing access_token")
	}

	// Use returned instance_url if provided, otherwise keep the original.
	finalURL := instanceURL
	if tokenResp.InstanceURL != "" {
		finalURL = parseBaseURL(tokenResp.InstanceURL)
	}

	orgID, err := fetchOrgID(ctx, finalURL, tokenResp.AccessToken)
	if err != nil {
		return nil, fmt.Errorf("fetching org ID: %w", err)
	}

	return &Credentials{
		AccessToken: tokenResp.AccessToken,
		InstanceURL: finalURL,
		TenantID:    orgID,
	}, nil
}

func fetchOrgID(ctx context.Context, instanceURL, accessToken string) (string, error) {
	userInfoURL := strings.TrimRight(instanceURL, "/") + "/services/oauth2/userinfo"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, userInfoURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating userinfo request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("userinfo request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading userinfo response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("userinfo request returned %d: %s", resp.StatusCode, string(body))
	}

	var userInfo struct {
		OrganizationID string `json:"organization_id"`
	}
	if err := json.Unmarshal(body, &userInfo); err != nil {
		return "", fmt.Errorf("parsing userinfo response: %w", err)
	}

	if userInfo.OrganizationID == "" {
		return "", fmt.Errorf("userinfo response missing organization_id")
	}
	return userInfo.OrganizationID, nil
}

func authenticateSOAP(ctx context.Context, instanceURL, username, password, apiVersion string) (*Credentials, error) {
	soapURL := strings.TrimRight(instanceURL, "/") + "/services/Soap/u/" + apiVersion + "/"

	soapBody := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8" ?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:urn="urn:partner.soap.sforce.com">
    <soapenv:Body>
        <urn:login>
            <urn:username><![CDATA[%s]]></urn:username>
            <urn:password><![CDATA[%s]]></urn:password>
        </urn:login>
    </soapenv:Body>
</soapenv:Envelope>`, username, password)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, soapURL, bytes.NewBufferString(soapBody))
	if err != nil {
		return nil, fmt.Errorf("creating SOAP request: %w", err)
	}
	req.Header.Set("Content-Type", "text/xml")
	req.Header.Set("SOAPAction", "Login")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SOAP login request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading SOAP response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("SOAP login returned %d: %s", resp.StatusCode, string(body))
	}

	return parseSOAPLoginResponse(body)
}

// parseSOAPLoginResponse extracts session ID, server URL, and org ID from the SOAP response.
func parseSOAPLoginResponse(body []byte) (*Credentials, error) {
	var envelope soapEnvelope
	if err := xml.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("parsing SOAP XML: %w", err)
	}

	result := envelope.Body.LoginResponse.Result
	if result.SessionID == "" {
		return nil, fmt.Errorf("SOAP response missing sessionId")
	}

	serverURL := parseBaseURL(result.ServerURL)

	orgID := result.UserInfo.OrganizationID
	if orgID == "" {
		return nil, fmt.Errorf("SOAP response missing organizationId")
	}

	return &Credentials{
		AccessToken: result.SessionID,
		InstanceURL: serverURL,
		TenantID:    orgID,
	}, nil
}

// SOAP XML structures for unmarshaling.
type soapEnvelope struct {
	XMLName xml.Name `xml:"Envelope"`
	Body    soapBody `xml:"Body"`
}

type soapBody struct {
	LoginResponse soapLoginResponse `xml:"loginResponse"`
}

type soapLoginResponse struct {
	Result soapLoginResult `xml:"result"`
}

type soapLoginResult struct {
	SessionID string       `xml:"sessionId"`
	ServerURL string       `xml:"serverUrl"`
	UserInfo  soapUserInfo `xml:"userInfo"`
}

type soapUserInfo struct {
	OrganizationID string `xml:"organizationId"`
}

// parseBaseURL extracts the scheme and host from a full URL.
func parseBaseURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Scheme + "://" + u.Host
}
