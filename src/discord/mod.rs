use serenity::http::Http;
use serenity::model::{channel::Message, id::ChannelId};

pub struct Bot {
    http: Http,
    channel_id: ChannelId,
}

impl Bot {
    pub fn new(token: &str, channel_id: u64) -> Self {
        let http = Http::new(token);
        Bot {
            http,
            channel_id: ChannelId::new(channel_id),
        }
    }

    pub async fn notify_livestream(
        &self,
        name: &str,
        title: &str,
        url: &str,
    ) -> serenity::Result<Message> {
        self.channel_id
            .say(
                &self.http,
                format!("「{}」配信中！ - {}\n{}", name, title, url),
            )
            .await
    }
}



