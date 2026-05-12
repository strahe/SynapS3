package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/urfave/cli/v3"
)

const (
	defaultAdminTimeout      = 10 * time.Second
	adminSettingsWriteHeader = "X-SynapS3-Settings-Write"
	adminSettingsWriteValue  = "1"
)

type adminCommandOptions struct {
	AdminURL   string
	ConfigPath string
	ConfigSet  bool
	Timeout    time.Duration
	JSON       bool
}

func adminCommand() *cli.Command {
	return &cli.Command{
		Name:  "admin",
		Usage: "operate SynapS3 through the admin API",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "admin-url", Usage: "admin API base URL; defaults to admin.addr from config"},
			&cli.BoolFlag{Name: "json", Usage: "output successful responses as JSON"},
			&cli.DurationFlag{Name: "timeout", Value: defaultAdminTimeout, Usage: "admin API request timeout"},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			return cli.ShowSubcommandHelp(cmd)
		},
		Commands: []*cli.Command{
			adminStatusCommand(),
			adminS3UserCommand(),
			adminSettingsCommand(),
			adminTaskCommand(),
		},
	}
}

func adminStatusCommand() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "show admin health and runtime status",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			client, opts, err := newAdminClientFromCommand(ctx, cmd)
			if err != nil {
				return err
			}
			health, statusCode, err := client.getHealth(ctx)
			if err != nil {
				return err
			}
			result := map[string]any{"health": health}

			switch health.Status {
			case "setup":
				var settings adminSettingsResponse
				if err := client.getJSON(ctx, "/api/v1/settings", &settings); err != nil {
					return err
				}
				result["settings"] = settings
				if opts.JSON {
					return writeAdminJSON(cmd.Root().Writer, result)
				}
				return writeAdminSetupStatus(cmd.Root().Writer, health, settings)
			case "ok":
				var system adminSystemInfo
				if err := client.getJSON(ctx, "/api/v1/system/info", &system); err != nil {
					return err
				}
				var workers adminWorkersResponse
				if err := client.getJSON(ctx, "/api/v1/workers", &workers); err != nil {
					return err
				}
				var cache adminCacheStats
				if err := client.getJSON(ctx, "/api/v1/cache/stats", &cache); err != nil {
					return err
				}
				result["system"] = system
				result["workers"] = workers
				result["cache"] = cache
				if opts.JSON {
					return writeAdminJSON(cmd.Root().Writer, result)
				}
				return writeAdminReadyStatus(cmd.Root().Writer, health, system, workers, cache)
			default:
				if opts.JSON && statusCode < 400 {
					return writeAdminJSON(cmd.Root().Writer, result)
				}
				_ = writeAdminHealthText(cmd.Root().ErrWriter, health)
				return fmt.Errorf("admin health is %s", health.Status)
			}
		},
	}
}

func adminS3UserCommand() *cli.Command {
	return &cli.Command{
		Name:  "s3-user",
		Usage: "manage S3 users",
		Commands: []*cli.Command{
			{
				Name:  "list",
				Usage: "list S3 users",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					var users []adminS3User
					if err := client.getJSON(ctx, "/api/v1/s3-users", &users); err != nil {
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, users)
					}
					return writeAdminS3UsersTable(cmd.Root().Writer, users)
				},
			},
			{
				Name:  "create",
				Usage: "create an S3 user",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "role", Usage: "S3 user role: user, userplus, or admin"},
					&cli.BoolFlag{Name: "yes", Usage: "confirm high-risk admin user creation"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					role := strings.TrimSpace(cmd.String("role"))
					if err := validateAdminRole(role, true); err != nil {
						return err
					}
					if role == "admin" && !cmd.Bool("yes") {
						return errors.New("creating an admin S3 user requires --yes")
					}
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					payload := map[string]string{}
					if role != "" {
						payload["role"] = role
					}
					var created adminS3Credentials
					if err := client.postJSON(ctx, "/api/v1/s3-users", payload, &created, true); err != nil {
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, created)
					}
					return writeAdminCredentials(cmd.Root().Writer, created)
				},
			},
			{
				Name:      "update",
				Usage:     "update an S3 user's role",
				ArgsUsage: "<access-key>",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "role", Usage: "S3 user role: user, userplus, or admin"},
					&cli.BoolFlag{Name: "yes", Usage: "confirm admin role assignment"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					accessKey, err := requireSingleArg(cmd, "access key")
					if err != nil {
						return err
					}
					role := strings.TrimSpace(cmd.String("role"))
					if err := validateAdminRole(role, false); err != nil {
						return err
					}
					if role == "admin" && !cmd.Bool("yes") {
						return errors.New("assigning the admin role requires --yes")
					}
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					var updated adminS3User
					path := "/api/v1/s3-users/" + url.PathEscape(accessKey)
					if err := client.putJSON(ctx, path, map[string]string{"role": role}, &updated, true); err != nil {
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, updated)
					}
					return writeAdminS3UsersTable(cmd.Root().Writer, []adminS3User{updated})
				},
			},
			{
				Name:      "rotate-secret",
				Usage:     "rotate an S3 user's secret key",
				ArgsUsage: "<access-key>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					accessKey, err := requireSingleArg(cmd, "access key")
					if err != nil {
						return err
					}
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					var rotated adminS3Credentials
					path := "/api/v1/s3-users/" + url.PathEscape(accessKey) + "/secret"
					if err := client.postJSON(ctx, path, map[string]any{}, &rotated, true); err != nil {
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, rotated)
					}
					return writeAdminCredentials(cmd.Root().Writer, rotated)
				},
			},
			{
				Name:      "delete",
				Usage:     "delete an S3 user",
				ArgsUsage: "<access-key>",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "yes", Usage: "confirm S3 user deletion"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if !cmd.Bool("yes") {
						return errors.New("deleting an S3 user requires --yes")
					}
					accessKey, err := requireSingleArg(cmd, "access key")
					if err != nil {
						return err
					}
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					path := "/api/v1/s3-users/" + url.PathEscape(accessKey)
					if err := client.deleteJSON(ctx, path, true); err != nil {
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, map[string]string{"status": "deleted"})
					}
					_, err = fmt.Fprintf(cmd.Root().Writer, "S3 user deleted: %s\n", accessKey)
					return err
				},
			},
		},
	}
}

func adminSettingsCommand() *cli.Command {
	return &cli.Command{
		Name:  "settings",
		Usage: "inspect and update runtime settings",
		Commands: []*cli.Command{
			{
				Name:      "get",
				Usage:     "show settings",
				ArgsUsage: "[field]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					field := strings.TrimSpace(cmd.Args().First())
					if cmd.Args().Len() > 1 {
						return errors.New("settings get accepts at most one field")
					}
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					var settings adminSettingsResponse
					if err := client.getJSON(ctx, "/api/v1/settings", &settings); err != nil {
						return err
					}
					if field != "" {
						value, err := adminSettingsFieldValue(settings, field)
						if err != nil {
							return err
						}
						if opts.JSON {
							return writeAdminJSON(cmd.Root().Writer, map[string]any{field: value})
						}
						_, err = fmt.Fprintf(cmd.Root().Writer, "%v\n", value)
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, settings)
					}
					return writeAdminSettingsSummary(cmd.Root().Writer, settings)
				},
			},
			{
				Name:      "set",
				Usage:     "update settings with field=value pairs",
				ArgsUsage: "<field=value>...",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "yes", Usage: "confirm settings changes that require review"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Len() == 0 {
						return errors.New("settings set requires at least one field=value pair")
					}
					updates, err := parseAdminSettingsUpdates(cmd.Args().Slice())
					if err != nil {
						return err
					}
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					var current adminSettingsResponse
					if err := client.getJSON(ctx, "/api/v1/settings", &current); err != nil {
						return err
					}
					if err := rejectEnvManagedSettings(current, updates.fields); err != nil {
						return err
					}
					reviewRequired := reviewRequiredSettingsChanges(current, updates.values)
					if len(reviewRequired) > 0 && !cmd.Bool("yes") {
						return fmt.Errorf("settings change requires --yes: %s", strings.Join(reviewRequired, ", "))
					}
					var saved adminSettingsResponse
					if err := client.putJSON(ctx, "/api/v1/settings", updates.payload, &saved, true); err != nil {
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, saved)
					}
					_, err = fmt.Fprintf(cmd.Root().Writer, "Settings saved\nRestart required: %s\n", formatAdminYesNo(saved.RestartRequired))
					return err
				},
			},
		},
	}
}

func adminTaskCommand() *cli.Command {
	return &cli.Command{
		Name:  "task",
		Usage: "inspect and retry background tasks",
		Commands: []*cli.Command{
			{
				Name:  "list",
				Usage: "list background tasks",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "type", Usage: "filter by task type"},
					&cli.StringFlag{Name: "stage", Usage: "filter by task stage; requires --type"},
					&cli.StringFlag{Name: "status", Usage: "filter by task status"},
					&cli.IntFlag{Name: "limit", Value: 20, Usage: "maximum tasks to return"},
					&cli.IntFlag{Name: "offset", Usage: "task list offset"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					taskType := strings.TrimSpace(cmd.String("type"))
					stage := strings.TrimSpace(cmd.String("stage"))
					if stage != "" && taskType == "" {
						return errors.New("--stage requires --type")
					}
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					query := url.Values{}
					if taskType != "" {
						query.Set("type", taskType)
					}
					if stage != "" {
						query.Set("stage", stage)
					}
					if status := strings.TrimSpace(cmd.String("status")); status != "" {
						query.Set("status", status)
					}
					if cmd.IsSet("limit") {
						query.Set("limit", strconv.Itoa(cmd.Int("limit")))
					}
					if cmd.IsSet("offset") {
						query.Set("offset", strconv.Itoa(cmd.Int("offset")))
					}
					path := "/api/v1/tasks"
					if encoded := query.Encode(); encoded != "" {
						path += "?" + encoded
					}
					var tasks adminTaskListResponse
					if err := client.getJSON(ctx, path, &tasks); err != nil {
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, tasks)
					}
					return writeAdminTasksTable(cmd.Root().Writer, tasks.Tasks)
				},
			},
			{
				Name:  "stats",
				Usage: "show task status counts",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					var stats []adminTaskStatusCount
					if err := client.getJSON(ctx, "/api/v1/tasks/stats", &stats); err != nil {
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, stats)
					}
					return writeAdminTaskStatsTable(cmd.Root().Writer, stats)
				},
			},
			{
				Name:      "retry",
				Usage:     "retry an exhausted task",
				ArgsUsage: "<id>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					taskID, err := requireSingleArg(cmd, "task id")
					if err != nil {
						return err
					}
					if _, err := strconv.ParseInt(taskID, 10, 64); err != nil {
						return fmt.Errorf("invalid task id %q", taskID)
					}
					client, opts, err := newAdminClientFromCommand(ctx, cmd)
					if err != nil {
						return err
					}
					var resp map[string]string
					if err := client.postJSON(ctx, "/api/v1/tasks/"+url.PathEscape(taskID)+"/retry", nil, &resp, false); err != nil {
						return err
					}
					if opts.JSON {
						return writeAdminJSON(cmd.Root().Writer, resp)
					}
					_, err = fmt.Fprintf(cmd.Root().Writer, "Task %s %s\n", taskID, resp["status"])
					return err
				},
			},
		},
	}
}

type adminAPIClient struct {
	baseURL    string
	httpClient *http.Client
}

func newAdminClientFromCommand(ctx context.Context, cmd *cli.Command) (*adminAPIClient, adminCommandOptions, error) {
	opts := adminOptionsFromCommand(cmd)
	baseURL, err := resolveAdminBaseURL(ctx, opts)
	if err != nil {
		return nil, adminCommandOptions{}, err
	}
	return &adminAPIClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: opts.Timeout,
		},
	}, opts, nil
}

func adminOptionsFromCommand(cmd *cli.Command) adminCommandOptions {
	root := cmd.Root()
	timeout := cmd.Duration("timeout")
	if timeout <= 0 {
		timeout = defaultAdminTimeout
	}
	return adminCommandOptions{
		AdminURL:   cmd.String("admin-url"),
		ConfigPath: root.String("config"),
		ConfigSet:  root.IsSet("config"),
		Timeout:    timeout,
		JSON:       cmd.Bool("json"),
	}
}

func resolveAdminBaseURL(_ context.Context, opts adminCommandOptions) (string, error) {
	if strings.TrimSpace(opts.AdminURL) != "" {
		return normalizeAdminBaseURL(opts.AdminURL)
	}

	src, err := config.ResolveSource(opts.ConfigPath, opts.ConfigSet)
	if err != nil {
		return "", err
	}
	cfg, err := config.LoadSource(src)
	if err != nil {
		return "", fmt.Errorf("loading config for admin URL: %w", err)
	}
	return normalizeAdminBaseURL(cfg.Admin.Addr)
}

func normalizeAdminBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("admin URL is empty")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parsing admin URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("admin URL scheme must be http or https")
	}
	host := u.Hostname()
	port := u.Port()
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		u.Host = "[" + host + "]"
	} else {
		u.Host = host
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func (c *adminAPIClient) getJSON(ctx context.Context, path string, out any) error {
	return c.doJSON(ctx, http.MethodGet, path, nil, out, false)
}

func (c *adminAPIClient) postJSON(ctx context.Context, path string, body any, out any, writeHeader bool) error {
	return c.doJSON(ctx, http.MethodPost, path, body, out, writeHeader)
}

func (c *adminAPIClient) putJSON(ctx context.Context, path string, body any, out any, writeHeader bool) error {
	return c.doJSON(ctx, http.MethodPut, path, body, out, writeHeader)
}

func (c *adminAPIClient) deleteJSON(ctx context.Context, path string, writeHeader bool) error {
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil, writeHeader)
}

func (c *adminAPIClient) getHealth(ctx context.Context) (adminHealthResponse, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/healthz"), nil)
	if err != nil {
		return adminHealthResponse{}, 0, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return adminHealthResponse{}, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	var health adminHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return adminHealthResponse{}, resp.StatusCode, fmt.Errorf("decoding health response: %w", err)
	}
	return health, resp.StatusCode, nil
}

func (c *adminAPIClient) doJSON(ctx context.Context, method, path string, body any, out any, writeHeader bool) error {
	var reader io.Reader
	hasBody := body != nil
	if hasBody {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encoding request JSON: %w", err)
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path), reader)
	if err != nil {
		return err
	}
	if hasBody || writeHeader {
		req.Header.Set("Content-Type", "application/json")
	}
	if writeHeader {
		req.Header.Set(adminSettingsWriteHeader, adminSettingsWriteValue)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeAdminAPIError(resp)
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decoding response JSON: %w", err)
	}
	return nil
}

func (c *adminAPIClient) endpoint(path string) string {
	if strings.HasPrefix(path, "/") {
		return c.baseURL + path
	}
	return c.baseURL + "/" + path
}

type adminAPIError struct {
	status  int
	Message string            `json:"error"`
	Fields  []adminFieldError `json:"fields,omitempty"`
}

type adminFieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e adminAPIError) Error() string {
	parts := []string{}
	if e.Message != "" {
		parts = append(parts, e.Message)
	} else {
		parts = append(parts, fmt.Sprintf("admin API error: %d", e.status))
	}
	for _, field := range e.Fields {
		parts = append(parts, field.Field+": "+field.Message)
	}
	return strings.Join(parts, " - ")
}

func decodeAdminAPIError(resp *http.Response) error {
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("admin API error %d: reading body: %w", resp.StatusCode, err)
	}
	var apiErr adminAPIError
	if err := json.Unmarshal(data, &apiErr); err != nil || (apiErr.Message == "" && len(apiErr.Fields) == 0) {
		text := strings.TrimSpace(string(data))
		if text == "" {
			text = http.StatusText(resp.StatusCode)
		}
		return fmt.Errorf("admin API error %d: %s", resp.StatusCode, text)
	}
	apiErr.status = resp.StatusCode
	return apiErr
}

type adminHealthResponse struct {
	Status string   `json:"status"`
	Errors []string `json:"errors,omitempty"`
}

type adminSystemInfo struct {
	Version       string `json:"version"`
	Commit        string `json:"commit"`
	BuildDate     string `json:"build_date"`
	UptimeSeconds int64  `json:"uptime_seconds"`
}

type adminWorkersResponse struct {
	Workers map[string]bool `json:"workers"`
}

type adminCacheStats struct {
	UsedBytes int64 `json:"used_bytes"`
	MaxBytes  int64 `json:"max_bytes"`
}

type adminS3User struct {
	AccessKey   string `json:"access_key"`
	Role        string `json:"role"`
	BucketCount int    `json:"bucket_count"`
}

type adminS3Credentials struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Role      string `json:"role"`
}

type adminSettingsResponse struct {
	Mode             string                    `json:"mode"`
	ConfigPath       string                    `json:"config_path"`
	Writable         bool                      `json:"writable"`
	RestartRequired  bool                      `json:"restart_required"`
	Config           adminSettingsConfig       `json:"config"`
	EnvManaged       map[string]string         `json:"env_managed"`
	ValidationErrors []adminFieldError         `json:"validation_errors,omitempty"`
	S3Users          adminSettingsS3UsersState `json:"s3_users"`
}

type adminSettingsS3UsersState struct {
	Available bool   `json:"available"`
	Reason    string `json:"reason,omitempty"`
}

type adminSettingsConfig struct {
	Server   adminSettingsServerConfig   `json:"server"`
	S3       adminSettingsS3Config       `json:"s3"`
	Filecoin adminSettingsFilecoinConfig `json:"filecoin"`
	Cache    adminSettingsCacheConfig    `json:"cache"`
	Worker   adminSettingsWorkerConfig   `json:"worker"`
	Logging  adminSettingsLoggingConfig  `json:"logging"`
}

type adminSettingsServerConfig struct {
	Port           string                 `json:"port"`
	TLS            adminSettingsTLSConfig `json:"tls"`
	MaxConnections int                    `json:"max_connections"`
	MaxRequests    int                    `json:"max_requests"`
}

type adminSettingsTLSConfig struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

type adminSettingsS3Config struct {
	Region string `json:"region"`
}

type adminSettingsFilecoinConfig struct {
	Network              string `json:"network"`
	RPCURL               string `json:"rpc_url"`
	Source               string `json:"source"`
	WithCDN              bool   `json:"with_cdn"`
	AllowPrivateNetworks bool   `json:"allow_private_networks"`
	DefaultCopies        int    `json:"default_copies"`
}

type adminSettingsCacheConfig struct {
	Dir            string `json:"dir"`
	MaxSizeGB      int    `json:"max_size_gb"`
	EvictionPolicy string `json:"eviction_policy"`
}

type adminSettingsWorkerConfig struct {
	Upload         adminSettingsWorkerPoolConfig `json:"upload"`
	Evictor        adminSettingsWorkerPoolConfig `json:"evictor"`
	StorageCleanup adminSettingsWorkerPoolConfig `json:"storage_cleanup"`
}

type adminSettingsWorkerPoolConfig struct {
	Concurrency  int    `json:"concurrency"`
	PollInterval string `json:"poll_interval"`
	MaxRetries   int    `json:"max_retries"`
}

type adminSettingsLoggingConfig struct {
	Level    string                             `json:"level"`
	Format   string                             `json:"format"`
	S3Access adminSettingsLoggingS3AccessConfig `json:"s3_access"`
}

type adminSettingsLoggingS3AccessConfig struct {
	Enabled bool   `json:"enabled"`
	Level   string `json:"level"`
}

type adminTaskListResponse struct {
	Tasks  []adminTaskItem `json:"tasks"`
	Total  int             `json:"total"`
	Limit  int             `json:"limit"`
	Offset int             `json:"offset"`
}

type adminTaskItem struct {
	ID            int64   `json:"id"`
	Type          string  `json:"type"`
	Stage         *string `json:"stage,omitempty"`
	RefType       string  `json:"ref_type"`
	RefID         int64   `json:"ref_id"`
	RefVersionID  string  `json:"ref_version_id"`
	Status        string  `json:"status"`
	RetryCount    int     `json:"retry_count"`
	MaxRetries    int     `json:"max_retries"`
	LastError     *string `json:"last_error,omitempty"`
	StatusMessage *string `json:"status_message,omitempty"`
	WaitReason    *string `json:"wait_reason,omitempty"`
	ScheduledAt   string  `json:"scheduled_at"`
}

type adminTaskStatusCount struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	Count  int64  `json:"count"`
}

type adminSettingKind int

const (
	adminSettingString adminSettingKind = iota
	adminSettingInt
	adminSettingBool
)

type adminSettingSpec struct {
	path []string
	kind adminSettingKind
}

var adminEditableSettings = map[string]adminSettingSpec{
	"server.port":                          {path: []string{"server", "port"}, kind: adminSettingString},
	"server.max_connections":               {path: []string{"server", "max_connections"}, kind: adminSettingInt},
	"server.max_requests":                  {path: []string{"server", "max_requests"}, kind: adminSettingInt},
	"server.tls.enabled":                   {path: []string{"server", "tls", "enabled"}, kind: adminSettingBool},
	"server.tls.cert_file":                 {path: []string{"server", "tls", "cert_file"}, kind: adminSettingString},
	"server.tls.key_file":                  {path: []string{"server", "tls", "key_file"}, kind: adminSettingString},
	"s3.region":                            {path: []string{"s3", "region"}, kind: adminSettingString},
	"filecoin.network":                     {path: []string{"filecoin", "network"}, kind: adminSettingString},
	"filecoin.rpc_url":                     {path: []string{"filecoin", "rpc_url"}, kind: adminSettingString},
	"filecoin.source":                      {path: []string{"filecoin", "source"}, kind: adminSettingString},
	"filecoin.with_cdn":                    {path: []string{"filecoin", "with_cdn"}, kind: adminSettingBool},
	"filecoin.allow_private_networks":      {path: []string{"filecoin", "allow_private_networks"}, kind: adminSettingBool},
	"filecoin.default_copies":              {path: []string{"filecoin", "default_copies"}, kind: adminSettingInt},
	"cache.dir":                            {path: []string{"cache", "dir"}, kind: adminSettingString},
	"cache.max_size_gb":                    {path: []string{"cache", "max_size_gb"}, kind: adminSettingInt},
	"cache.eviction_policy":                {path: []string{"cache", "eviction_policy"}, kind: adminSettingString},
	"worker.upload.concurrency":            {path: []string{"worker", "upload", "concurrency"}, kind: adminSettingInt},
	"worker.upload.poll_interval":          {path: []string{"worker", "upload", "poll_interval"}, kind: adminSettingString},
	"worker.upload.max_retries":            {path: []string{"worker", "upload", "max_retries"}, kind: adminSettingInt},
	"worker.evictor.concurrency":           {path: []string{"worker", "evictor", "concurrency"}, kind: adminSettingInt},
	"worker.evictor.poll_interval":         {path: []string{"worker", "evictor", "poll_interval"}, kind: adminSettingString},
	"worker.evictor.max_retries":           {path: []string{"worker", "evictor", "max_retries"}, kind: adminSettingInt},
	"worker.storage_cleanup.concurrency":   {path: []string{"worker", "storage_cleanup", "concurrency"}, kind: adminSettingInt},
	"worker.storage_cleanup.poll_interval": {path: []string{"worker", "storage_cleanup", "poll_interval"}, kind: adminSettingString},
	"worker.storage_cleanup.max_retries":   {path: []string{"worker", "storage_cleanup", "max_retries"}, kind: adminSettingInt},
	"logging.level":                        {path: []string{"logging", "level"}, kind: adminSettingString},
	"logging.format":                       {path: []string{"logging", "format"}, kind: adminSettingString},
	"logging.s3_access.enabled":            {path: []string{"logging", "s3_access", "enabled"}, kind: adminSettingBool},
	"logging.s3_access.level":              {path: []string{"logging", "s3_access", "level"}, kind: adminSettingString},
}

type adminSettingsUpdates struct {
	payload map[string]any
	values  map[string]any
	fields  []string
}

func parseAdminSettingsUpdates(args []string) (adminSettingsUpdates, error) {
	payload := map[string]any{}
	values := map[string]any{}
	fields := make([]string, 0, len(args))
	for _, arg := range args {
		field, raw, ok := strings.Cut(arg, "=")
		if !ok || strings.TrimSpace(field) == "" {
			return adminSettingsUpdates{}, fmt.Errorf("invalid setting %q, expected field=value", arg)
		}
		field = strings.TrimSpace(field)
		spec, ok := adminEditableSettings[field]
		if !ok {
			return adminSettingsUpdates{}, fmt.Errorf("setting %q is not editable through admin settings", field)
		}
		value, err := parseAdminSettingValue(field, raw, spec.kind)
		if err != nil {
			return adminSettingsUpdates{}, err
		}
		setNestedAdminValue(payload, spec.path, value)
		values[field] = value
		fields = append(fields, field)
	}
	return adminSettingsUpdates{payload: payload, values: values, fields: fields}, nil
}

func parseAdminSettingValue(field, raw string, kind adminSettingKind) (any, error) {
	switch kind {
	case adminSettingString:
		return raw, nil
	case adminSettingInt:
		value, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("%s must be an integer", field)
		}
		return value, nil
	case adminSettingBool:
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("%s must be true or false", field)
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported setting kind for %s", field)
	}
}

func setNestedAdminValue(root map[string]any, path []string, value any) {
	current := root
	for _, part := range path[:len(path)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[part] = next
		}
		current = next
	}
	current[path[len(path)-1]] = value
}

func rejectEnvManagedSettings(settings adminSettingsResponse, fields []string) error {
	for _, field := range fields {
		if envName, ok := settings.EnvManaged[field]; ok {
			return fmt.Errorf("setting %s is managed by %s", field, envName)
		}
	}
	return nil
}

func reviewRequiredSettingsChanges(current adminSettingsResponse, values map[string]any) []string {
	var fields []string
	if value, ok := values["server.max_connections"].(int); ok && value > current.Config.Server.MaxConnections {
		fields = append(fields, "server.max_connections")
	}
	if value, ok := values["server.max_requests"].(int); ok && value > current.Config.Server.MaxRequests {
		fields = append(fields, "server.max_requests")
	}
	if value, ok := values["filecoin.network"].(string); ok &&
		!strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(current.Config.Filecoin.Network)) {
		fields = append(fields, "filecoin.network")
	}
	if value, ok := values["filecoin.allow_private_networks"].(bool); ok && value && !current.Config.Filecoin.AllowPrivateNetworks {
		fields = append(fields, "filecoin.allow_private_networks")
	}
	return fields
}

func adminSettingsFieldValue(settings adminSettingsResponse, field string) (any, error) {
	spec, ok := adminEditableSettings[field]
	if !ok {
		return nil, fmt.Errorf("setting %q is not available through settings get", field)
	}
	value, ok := adminSettingsPathValue(reflect.ValueOf(settings.Config), spec.path)
	if !ok {
		return nil, fmt.Errorf("setting %q is not available through settings get", field)
	}
	return value, nil
}

func adminSettingsPathValue(value reflect.Value, path []string) (any, bool) {
	current := value
	for _, part := range path {
		for current.Kind() == reflect.Pointer {
			if current.IsNil() {
				return nil, false
			}
			current = current.Elem()
		}
		if current.Kind() != reflect.Struct {
			return nil, false
		}
		next, ok := adminStructJSONFieldValue(current, part)
		if !ok {
			return nil, false
		}
		current = next
	}
	return current.Interface(), true
}

func adminStructJSONFieldValue(value reflect.Value, name string) (reflect.Value, bool) {
	valueType := value.Type()
	for i := range value.NumField() {
		field := valueType.Field(i)
		jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
		if jsonName == name {
			return value.Field(i), true
		}
	}
	return reflect.Value{}, false
}

func validateAdminRole(role string, allowEmpty bool) error {
	if role == "" && allowEmpty {
		return nil
	}
	switch role {
	case "user", "userplus", "admin":
		return nil
	case "":
		return errors.New("--role is required")
	default:
		return fmt.Errorf("invalid S3 user role %q", role)
	}
}

func requireSingleArg(cmd *cli.Command, label string) (string, error) {
	if cmd.Args().Len() != 1 {
		return "", fmt.Errorf("expected one %s argument", label)
	}
	value := strings.TrimSpace(cmd.Args().First())
	if value == "" {
		return "", fmt.Errorf("%s must be non-empty", label)
	}
	return value, nil
}

func writeAdminJSON(w io.Writer, value any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

type adminOutputRow struct {
	Name  string
	Value string
}

func writeAdminHealthText(w io.Writer, health adminHealthResponse) error {
	if _, err := fmt.Fprintf(w, "Status: %s\n", health.Status); err != nil {
		return err
	}
	if len(health.Errors) > 0 {
		if _, err := fmt.Fprintln(w, "Errors:"); err != nil {
			return err
		}
		for _, msg := range health.Errors {
			if _, err := fmt.Fprintf(w, "- %s\n", msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeAdminReadyStatus(w io.Writer, health adminHealthResponse, system adminSystemInfo, workers adminWorkersResponse, cache adminCacheStats) error {
	if _, err := fmt.Fprintln(w, "SynapS3 Admin"); err != nil {
		return err
	}
	if err := writeAdminRows(w, "", []adminOutputRow{
		{Name: "Status", Value: health.Status},
		{Name: "Version", Value: system.Version},
		{Name: "Commit", Value: system.Commit},
		{Name: "Uptime", Value: formatAdminDurationSeconds(system.UptimeSeconds)},
	}); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "Cache"); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "USED\tLIMIT\tUSAGE")
	_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", formatAdminBytes(cache.UsedBytes), formatAdminBytes(cache.MaxBytes), formatAdminPercent(cache.UsedBytes, cache.MaxBytes))
	if err := tw.Flush(); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "Workers"); err != nil {
		return err
	}
	tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NAME\tSTATUS")
	for _, name := range sortedAdminWorkerNames(workers.Workers) {
		_, _ = fmt.Fprintf(tw, "%s\t%s\n", name, formatAdminHealthStatus(workers.Workers[name]))
	}
	return tw.Flush()
}

func writeAdminSetupStatus(w io.Writer, health adminHealthResponse, settings adminSettingsResponse) error {
	if _, err := fmt.Fprintln(w, "SynapS3 Admin"); err != nil {
		return err
	}
	if err := writeAdminRows(w, "", []adminOutputRow{
		{Name: "Status", Value: health.Status},
		{Name: "Mode", Value: settings.Mode},
		{Name: "Config", Value: settings.ConfigPath},
		{Name: "Writable", Value: formatAdminYesNo(settings.Writable)},
		{Name: "Restart required", Value: formatAdminYesNo(settings.RestartRequired)},
	}); err != nil {
		return err
	}
	if len(settings.ValidationErrors) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "Validation Errors"); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "FIELD\tMESSAGE")
	for _, fieldErr := range settings.ValidationErrors {
		_, _ = fmt.Fprintf(tw, "%s\t%s\n", fieldErr.Field, fieldErr.Message)
	}
	return tw.Flush()
}

func writeAdminS3UsersTable(w io.Writer, users []adminS3User) error {
	if _, err := fmt.Fprintln(w, "S3 Users"); err != nil {
		return err
	}
	if len(users) == 0 {
		_, err := fmt.Fprintln(w, "No S3 users found.")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ACCESS_KEY\tROLE\tBUCKETS")
	for _, user := range users {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\n", user.AccessKey, user.Role, user.BucketCount)
	}
	return tw.Flush()
}

func writeAdminCredentials(w io.Writer, credentials adminS3Credentials) error {
	if _, err := fmt.Fprintln(w, "S3 User Credentials"); err != nil {
		return err
	}
	return writeAdminRows(w, "", []adminOutputRow{
		{Name: "Access key", Value: credentials.AccessKey},
		{Name: "Secret key", Value: credentials.SecretKey},
		{Name: "Role", Value: credentials.Role},
	})
}

func writeAdminSettingsSummary(w io.Writer, settings adminSettingsResponse) error {
	if _, err := fmt.Fprintln(w, "Settings"); err != nil {
		return err
	}
	if err := writeAdminRows(w, "", []adminOutputRow{
		{Name: "Mode", Value: settings.Mode},
		{Name: "Config", Value: settings.ConfigPath},
		{Name: "Writable", Value: formatAdminYesNo(settings.Writable)},
		{Name: "Restart required", Value: formatAdminYesNo(settings.RestartRequired)},
	}); err != nil {
		return err
	}
	sections := []struct {
		title string
		rows  []adminOutputRow
	}{
		{
			title: "Server",
			rows: []adminOutputRow{
				{Name: "server.port", Value: settings.Config.Server.Port},
				{Name: "server.max_connections", Value: strconv.Itoa(settings.Config.Server.MaxConnections)},
				{Name: "server.max_requests", Value: strconv.Itoa(settings.Config.Server.MaxRequests)},
				{Name: "server.tls.enabled", Value: formatAdminYesNo(settings.Config.Server.TLS.Enabled)},
			},
		},
		{
			title: "S3",
			rows: []adminOutputRow{
				{Name: "s3.region", Value: settings.Config.S3.Region},
			},
		},
		{
			title: "Filecoin",
			rows: []adminOutputRow{
				{Name: "filecoin.network", Value: settings.Config.Filecoin.Network},
				{Name: "filecoin.rpc_url", Value: settings.Config.Filecoin.RPCURL},
				{Name: "filecoin.source", Value: settings.Config.Filecoin.Source},
				{Name: "filecoin.with_cdn", Value: formatAdminYesNo(settings.Config.Filecoin.WithCDN)},
				{Name: "filecoin.allow_private_networks", Value: formatAdminYesNo(settings.Config.Filecoin.AllowPrivateNetworks)},
				{Name: "filecoin.default_copies", Value: strconv.Itoa(settings.Config.Filecoin.DefaultCopies)},
			},
		},
		{
			title: "Cache",
			rows: []adminOutputRow{
				{Name: "cache.dir", Value: settings.Config.Cache.Dir},
				{Name: "cache.max_size_gb", Value: formatAdminGiB(settings.Config.Cache.MaxSizeGB)},
				{Name: "cache.eviction_policy", Value: settings.Config.Cache.EvictionPolicy},
			},
		},
		{
			title: "Worker",
			rows: []adminOutputRow{
				{Name: "worker.upload.concurrency", Value: strconv.Itoa(settings.Config.Worker.Upload.Concurrency)},
				{Name: "worker.upload.poll_interval", Value: settings.Config.Worker.Upload.PollInterval},
				{Name: "worker.upload.max_retries", Value: strconv.Itoa(settings.Config.Worker.Upload.MaxRetries)},
				{Name: "worker.evictor.concurrency", Value: strconv.Itoa(settings.Config.Worker.Evictor.Concurrency)},
				{Name: "worker.evictor.poll_interval", Value: settings.Config.Worker.Evictor.PollInterval},
				{Name: "worker.evictor.max_retries", Value: strconv.Itoa(settings.Config.Worker.Evictor.MaxRetries)},
				{Name: "worker.storage_cleanup.concurrency", Value: strconv.Itoa(settings.Config.Worker.StorageCleanup.Concurrency)},
				{Name: "worker.storage_cleanup.poll_interval", Value: settings.Config.Worker.StorageCleanup.PollInterval},
				{Name: "worker.storage_cleanup.max_retries", Value: strconv.Itoa(settings.Config.Worker.StorageCleanup.MaxRetries)},
			},
		},
		{
			title: "Logging",
			rows: []adminOutputRow{
				{Name: "logging.level", Value: settings.Config.Logging.Level},
				{Name: "logging.format", Value: settings.Config.Logging.Format},
				{Name: "logging.s3_access.enabled", Value: formatAdminYesNo(settings.Config.Logging.S3Access.Enabled)},
				{Name: "logging.s3_access.level", Value: settings.Config.Logging.S3Access.Level},
			},
		},
	}
	for _, section := range sections {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeAdminRows(w, section.title, section.rows); err != nil {
			return err
		}
	}
	return nil
}

func writeAdminTasksTable(w io.Writer, tasks []adminTaskItem) error {
	if _, err := fmt.Fprintln(w, "Tasks"); err != nil {
		return err
	}
	if len(tasks) == 0 {
		_, err := fmt.Fprintln(w, "No tasks found.")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ID\tTYPE\tSTAGE\tSTATUS\tRETRIES\tREF\tSCHEDULED\tDETAILS")
	for _, task := range tasks {
		stage := ""
		if task.Stage != nil {
			stage = *task.Stage
		}
		details := adminTaskDetails(task)
		ref := task.RefType + ":" + strconv.FormatInt(task.RefID, 10)
		if task.RefVersionID != "" {
			ref += ":" + task.RefVersionID
		}
		retries := fmt.Sprintf("%d/%d", task.RetryCount, task.MaxRetries)
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", task.ID, task.Type, stage, task.Status, retries, ref, task.ScheduledAt, details)
	}
	return tw.Flush()
}

func adminTaskDetails(task adminTaskItem) string {
	switch {
	case task.StatusMessage != nil && *task.StatusMessage != "":
		if task.WaitReason != nil && *task.WaitReason != "" {
			return *task.WaitReason + ": " + *task.StatusMessage
		}
		return *task.StatusMessage
	case task.LastError != nil:
		return *task.LastError
	default:
		return ""
	}
}

func writeAdminTaskStatsTable(w io.Writer, stats []adminTaskStatusCount) error {
	if _, err := fmt.Fprintln(w, "Task Stats"); err != nil {
		return err
	}
	if len(stats) == 0 {
		_, err := fmt.Fprintln(w, "No task stats found.")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "TYPE\tSTATUS\tCOUNT")
	for _, stat := range stats {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\n", stat.Type, stat.Status, stat.Count)
	}
	return tw.Flush()
}

func writeAdminRows(w io.Writer, title string, rows []adminOutputRow) error {
	if title != "" {
		if _, err := fmt.Fprintln(w, title); err != nil {
			return err
		}
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(w, "%s: %s\n", row.Name, row.Value); err != nil {
			return err
		}
	}
	return nil
}

func sortedAdminWorkerNames(workers map[string]bool) []string {
	names := make([]string, 0, len(workers))
	for name := range workers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func formatAdminHealthStatus(ok bool) string {
	if ok {
		return "healthy"
	}
	return "unhealthy"
}

func formatAdminYesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func formatAdminDurationSeconds(seconds int64) string {
	if seconds < 0 {
		seconds = 0
	}
	return (time.Duration(seconds) * time.Second).String()
}

func formatAdminBytes(bytes int64) string {
	const unit = 1024
	if bytes > -unit && bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	value := float64(bytes)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB", "PiB"} {
		value /= unit
		absValue := value
		if absValue < 0 {
			absValue = -absValue
		}
		if absValue < unit {
			return fmt.Sprintf("%.2f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.2f EiB", value/unit)
}

func formatAdminGiB(value int) string {
	return fmt.Sprintf("%.2f GiB", float64(value))
}

func formatAdminPercent(used, max int64) string {
	if max <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f%%", float64(used)*100/float64(max))
}
