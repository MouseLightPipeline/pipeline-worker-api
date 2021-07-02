import * as amqp from "amqplib";
import {Connection, Channel} from "amqplib";
import {TaskExecution} from "../data-model/local/taskExecution";
import {MessageQueueService} from "../options/coreServicesOptions";
import {StatusChannel} from "./statusChannel";

const debug = require("debug")("pipeline:main-queue");

const TaskExecutionCompleteQueue = "TaskExecutionCompleteQueue";
const TaskExecutionUpdateQueue = "TaskExecutionUpdateQueue";

export class MainQueue {
    private static instance: MainQueue = new MainQueue();

    private _url: string;

    private _connection: Connection = null;
    private _channel: Channel = null;

    private _lastConnectAttempt: Date = null;

    private _statusChannel: StatusChannel = new StatusChannel();

    public static get Instance() {
        return this.instance;
    }

    public get StatusChannel(): StatusChannel {
        return this._statusChannel;
    }

    public async connect(): Promise<void> {
        if (!this._lastConnectAttempt || ((Date.now() - this._lastConnectAttempt.valueOf()) > 5000)) {
            this._url = `amqp://${MessageQueueService.host}:${MessageQueueService.port}`;
            debug(`main queue url: ${this._url}`);

            if (await this.createConnection()) {
                if ((await this.createTaskCompleteChannel())) {
                    await this._statusChannel.connect(this._connection);

                    this._lastConnectAttempt = new Date();

                    debug(`main queue ready`);
                }
            }
        } else {
            debug(`skipping connection - last attempt less than 5 seconds ago`);
        }
    }

    public async sendTaskExecutionComplete(taskExecution: TaskExecution) {
        try {
            if (!this._channel) {
                await this.connect();
            }

            if (this._channel) {
                this._channel.sendToQueue(TaskExecutionCompleteQueue, Buffer.from(JSON.stringify(taskExecution)), {persistent: true});
            } else {
                debug(`failed to send task execution complete ${taskExecution.id}`);
            }
        }
        catch (err) {
            debug(err);
        }
    }

    public async sendTaskExecutionUpdate(taskExecution: TaskExecution) {
        try {
            if (!this._channel) {
                await this.connect();
            }

            if (this._channel) {
                this._channel.sendToQueue(TaskExecutionUpdateQueue, Buffer.from(JSON.stringify(taskExecution)), {persistent: false});
            } else {
                debug(`failed to send task execution update ${taskExecution.id}`);
            }
        }
        catch (err) {
            debug(err);
        }
    }

    private async createConnection(): Promise<boolean> {
        try {
            this._connection = await amqp.connect(this._url);

            this._connection.on("error", (err) => {
                // From amqp docs - closed with be called for error - no need for cleanup here.
                debug(err);
            });

            this._connection.on("close", () => {
                debug("connection closed");

                this._connection = null;
                this._channel = null;
            });

            return true;
        } catch (err) {
            this._connection = null;
            debug("failed to establish connection");
            debug(err);
        }

        return false;
    }

    private async createTaskCompleteChannel(): Promise<boolean> {
        try {
            this._channel = await this._connection.createChannel();

            await this._channel.assertQueue(TaskExecutionCompleteQueue, {durable: true});

            await this._channel.assertQueue(TaskExecutionUpdateQueue, {durable: false});

            return true;
        } catch (err) {
            if (this._connection) {
                // May have already closed from error above.
                await this._connection.close();
            }

            debug("failed to establish channel");
            debug(err);
        }

        return false;
    }
}
