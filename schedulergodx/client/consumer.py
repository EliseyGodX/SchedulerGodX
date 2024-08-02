import schedulergodx.utils as utils


class Consumer(utils.AbstractionConnectClass):
    
    def get_response(self, message_id: utils.MessageId) -> utils.MessageDisassemble | None:
        self.channel.tx_select()
        while True:
            method_frame, header_frame, body = self.channel.basic_get(queue=self.queue, auto_ack=False)
            if method_frame:
                message = utils.MessageConstructor.disassemble(body)
                if message.metadata['id'] == message_id:
                    self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    return message
                else:
                    self.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
            else:
                break
        self.channel.tx_commit()
        return None