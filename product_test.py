from apiproxy.douyin.douyin import Douyin
from apiproxy.douyin import douyin_headers
import requests,json
import pandas as pd
import numpy as np
# import modin.pandas as pd #pip install "modin[all]"
# import ray
# ray.init()

dy = Douyin()

def douyin(content_link_str: str):
    # 视频
    # 8.76 s@r.RK Njp:/ 02/15  我坚信：天道酬勤。# 张大大 # 努力 # 那些年的笔记本  https://v.douyin.com/iRd3A4ut/ 复制此链接，打开Dou音搜索，直接观看视频！
    # link = dy.getShareLink('9.43 Tlp:/ m@D.Hv 07/28  5000年传承下来的 想象力和创造力 666啊 # 脑洞大开 # 地球村 # 搞笑# 那年那兔那些事  https://v.douyin.com/iRd7kTo5/ 复制此链接，打开Dou音搜索，直接观看视频！')
    # 图集
    # link = dy.getShareLink('8.76 s@r.RK Njp:/ 02/15  我坚信：天道酬勤。# 张大大 # 努力 # 那些年的笔记本  https://v.douyin.com/iRd3A4ut/ 复制此链接，打开Dou音搜索，直接观看视频！')
    link = dy.getShareLink(content_link_str)
    urlstr, awemeType, zuopin_id = dy.getKey(link)
    urlstr = urlstr.split("?")[0]
    if "iesdouyin.com" in urlstr:
        urlstr = urlstr.replace("iesdouyin.com", "douyin.com")
    awemeinfo = dy.getAwemeInfo(zuopin_id)
    # 尝试判断是否已经删除
    deleted = False
    if len(awemeinfo[1])== 0:
        deleted = "超过服务器请求次数，等待后重试"
        print('超过服务器请求次数，等待后重试')
        return [urlstr,deleted]
    try:
        # if awemeinfo[1].get("filter_detail")["filter_detail"]["notice"] == "抱歉，作品不见了" or awemeinfo[1]["filter_detail"]["filter_reason"] == "status_audit_self_see" or awemeinfo[1]["filter_detail"]["filter_reason"] == 'status_audit_not_pass' or awemeinfo[1]["filter_detail"]["filter_reason"] == 'author_level_ban&status_audit_self_see':
        if "filter_detail" in awemeinfo[1]:
            deleted = True
            with open(f'{zuopin_id}_failed.json', 'w') as f:
                json.dump(awemeinfo[1], f, indent=2,ensure_ascii=False)
            return [urlstr, deleted]
        # if awemeinfo[1]["filter_detail"]["detail_msg"] == "由于对方隐私设置，无法查看，去看看其他作品吧":
        #     deleted = "隐藏"
        #     return [urlstr,deleted]
    except:
        pass
    sec_uid = awemeinfo[0]["author"]["sec_uid"]  # 供请求用户信息时使用
    userinfo = dy.getUserDetailInfo(sec_uid)
    awemeinfo[0]["author"]["details_info"] = userinfo
    video_url = ""
    video_cover = ""
    pic_note_first = ""
    if awemeType == "video":
        video_url = awemeinfo[0]["video"]["play_addr"]["url_list"][0]  #  视频下载地址
        video_cover = awemeinfo[0]["video"]["cover"]["url_list"][0]  # 视频封面
    elif awemeType == "note":
        # pic_note
        pic_note_first = awemeinfo[0]["images"][0]["url_list"][0]  # 图集首张图片
    desc = awemeinfo[0]["desc"]  # 作品描述
    create_time = awemeinfo[0]["create_time"]  # 作品创建时间
    douyinhao = awemeinfo[0]["author"]["unique_id"]  # 作者抖音号
    nickname = awemeinfo[0]["author"]["nickname"]  # 作者昵称
    uid = awemeinfo[0]["author"]["uid"]  # 作者uid
    short_id = awemeinfo[0]["author"]["short_id"]  # 作者shortId
    province = awemeinfo[0]["author"]["details_info"]["user"]["province"]  # 作者省份
    city = awemeinfo[0]["author"]["details_info"]["user"]["city"]  # 作者城市
    age = awemeinfo[0]["author"]["details_info"]["user"]["user_age"]  # 作者年龄
    dianzan = awemeinfo[0]["statistics"]["digg_count"]  # 作品点赞数
    pinglun = awemeinfo[0]["statistics"]["comment_count"]  # 作品评论数
    shoucang = awemeinfo[0]["statistics"]["collect_count"]  # 作品收藏数
    return [urlstr, deleted, video_url, video_cover, pic_note_first, desc, create_time, dianzan, pinglun, shoucang,
                awemeType, zuopin_id, nickname, sec_uid, douyinhao, uid, short_id, province, city, age, awemeinfo[1],
                ]

def toutiao(url):
    r = requests.get(url)
    if "抱歉，你访问的内容不存在" in r.text:
        return True
    return False


def xigua(url):
    r = requests.get(url)
    if "非常抱歉！您查看的页面找不到了" in r.text:
        return True
    return False


df = pd.read_excel("芙蓉区.xlsx")
df["链接"] = df["链接"].str.strip()
# 删除链接为空的行
df.dropna(subset=["链接"], inplace=True)
df = df[df["链接"] != ""]
# 删除所有值均为 NaN 或空的列
df.dropna(axis=1, how="all", inplace=True)
df = df.loc[:, (df != "").all(axis=0)]

# 批量增加新的列
columns=["视频链接","视频封面链接","图集首张链接","作品描述","作品创建时间","作品点赞数","作品评论数","作品收藏数","作品类型","作品唯一ID","昵称","作者sec_uid","作者抖音号","作者UID","作者short_id","作者所在省份","作者所在城市","作者年龄","作品原始信息"]
for col in columns:
    df[col]=''
# df1=df1.append([["视频链接","视频封面链接","图集首张链接","作品描述","作品创建时间","作品点赞数","作品评论数","作品收藏数","作品类型","作品唯一ID","昵称","作者sec_uid","作者抖音号","作者UID","作者short_id","作者所在省份","作者所在城市","作者年龄","作品原始信息"]])
#df.append是要有变量接返回值的，如果你直接df.append()，print之后没有变化，请注意

# 迭代每一行
def dealwithrow(row):
    # try:
        link = row["链接"]
        # 头条
        if pd.notna(link) and "toutiao.com" in link:
            row["平台"] = "今日头条"
            if toutiao(link):
                row["已删帖"] = "已删帖"
            else:
                row["已删帖"] = "未删除"
        # 西瓜
        if pd.notna(link) and "ixigua.com" in link:
            row["平台"] = "西瓜视频"
            if xigua(link):
                row["已删帖"] = "已删帖"
            else:
                row["已删帖"] = "未删除"
        # 处理抖音
        if pd.notna(link) and "douyin" in link:
            row["平台"] = "抖音"
            if "user" in link:
                row["链接"] = "非标准链接\t"+link
                return row
            r = douyin(link)
            newlink = r[0]
            row["链接"] = newlink

            if r[1] == "超过服务器请求次数，等待后重试":
                return

            if r[1]:
                row["已删帖"] = "已删帖"
            elif r[1]=="隐藏":
                row["已删帖"] = "作者隐藏"
            else:
                row["已删帖"] = "未删除"
                row["视频链接"] , row["视频封面链接"] , row["图集首张链接"] , row["作品描述"] , row["作品创建时间"] , row["作品点赞数"] , row["作品评论数"] , row["作品收藏数"] , row["作品类型"] , row["作品唯一ID"] , row["昵称"] , row["作者sec_uid"] , row["作者抖音号"] , row["作者UID"] , row["作者short_id"] , row["作者所在省份"] , row["作者所在城市"] , row["作者年龄"] , row["作品原始信息"] = r[2:]
        if pd.notna(link) and "weibo.com" in link:
            row["平台"] = "微博"
        if pd.notna(link) and "huoshan.com" in link:
            row["平台"] = "抖音火山版"
        if pd.notna(link) and "xiaohongshu.com" in link:
            row["平台"] = "小红书"
        if pd.notna(link) and "//x.com" in link:
            row["平台"] = "推特"
        return row
    # except Exception as e:
    #     print("出现错误：", list(row), e)


df = df.apply(dealwithrow, axis=1)

# 使用 fillna() 将 NaN 值替换为一个不会影响排序的特定字符串，例如 'zzz'
df["已删帖"].fillna("zzz", inplace=True)

# 使用 sort_values() 按 'isdel' 列进行排序，确保 '已删除' 位于最前面
df.sort_values("已删帖", ascending=False, inplace=True)

# 使用 drop_duplicates() 去重，仅保留 'links' 列中第一次出现的记录
df.drop_duplicates(subset="链接", keep="first", inplace=True)

# 可以选择将 'zzz' 替换回 NaN，如果需要的话
df["已删帖"].replace("zzz", np.nan, inplace=True)
df.to_excel("result.xlsx", index=False)
